/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.replication.action.bulk

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import org.opensearch.replication.util.coroutineContext
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.ResourceNotFoundException
import org.opensearch.core.rest.RestStatus
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.admin.indices.get.GetIndexRequest
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.IndicesOptions
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.core.action.ActionListener
import org.opensearch.core.tasks.TaskId
import org.opensearch.replication.ReplicationSettings
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.metadata.ReplicationOverallState
import org.opensearch.replication.metadata.state.BulkReplicationTaskMetadata
import org.opensearch.replication.metadata.state.BulkTaskState
import org.opensearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import org.opensearch.replication.metadata.state.getReplicationStateParamsForIndex
import org.opensearch.replication.task.bulk.BulkOperationType
import org.opensearch.replication.task.bulk.BulkReplicationTask
import org.opensearch.replication.task.bulk.BulkReplicationTaskStatus
import org.opensearch.replication.task.bulk.FailedIndex
import org.opensearch.core.index.Index
import org.opensearch.core.index.shard.ShardId
import org.opensearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import org.opensearch.replication.action.index.ReplicateIndexRequest
import org.opensearch.replication.action.setup.SetupChecksAction
import org.opensearch.replication.action.setup.SetupChecksRequest
import org.opensearch.replication.metadata.store.ReplicationContext
import org.opensearch.replication.util.SecurityContext
import org.opensearch.replication.util.overrideFgacRole
import org.opensearch.replication.util.suspendExecute
import org.opensearch.replication.util.suspending
import org.opensearch.replication.util.waitForClusterStateUpdate
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.RestoreInProgress
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.common.settings.Settings
import org.opensearch.persistent.PersistentTasksService
import org.opensearch.replication.ReplicationPlugin
import org.opensearch.replication.task.index.IndexReplicationExecutor
import org.opensearch.replication.task.index.IndexReplicationParams
import org.opensearch.replication.util.StaleTaskUtils
import org.opensearch.replication.util.ValidationUtil
import org.opensearch.replication.util.startTask
import org.opensearch.common.unit.TimeValue
import org.opensearch.tasks.Task
import org.opensearch.tasks.TaskAwareRequest
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.Requests
import org.opensearch.action.admin.indices.open.OpenIndexRequest
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.commons.replication.action.StopIndexReplicationRequest
import org.opensearch.replication.action.index.block.IndexBlockUpdateType
import org.opensearch.replication.action.index.block.UpdateIndexBlockAction
import org.opensearch.replication.action.index.block.UpdateIndexBlockRequest
import org.opensearch.replication.action.stop.TransportStopIndexReplicationAction.StopReplicationTask
import org.opensearch.replication.action.status.ReplicationStatusAction
import org.opensearch.replication.action.status.ShardInfoRequest
import org.opensearch.replication.action.status.ShardInfoResponse
import org.opensearch.replication.metadata.UpdateMetadataAction
import org.opensearch.replication.metadata.UpdateMetadataRequest
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

open class TransportBulkReplicationAction(
    actionName: String,
    private val operationType: BulkOperationType,
    transportService: TransportService,
    actionFilters: ActionFilters,
    private val client: Client,
    private val clusterService: ClusterService,
    private val threadPool: ThreadPool,
    private val indexNameExpressionResolver: IndexNameExpressionResolver,
    private val replicationMetadataManager: ReplicationMetadataManager,
    private val replicationSettings: ReplicationSettings,
    private val persistentTasksService: PersistentTasksService,
    private val replicateIndexAction: org.opensearch.replication.action.index.TransportReplicateIndexAction,
    private val clusterManagerAction: org.opensearch.replication.action.index.TransportReplicateIndexClusterManagerNodeAction
) : HandledTransportAction<BulkReplicationRequest, BulkReplicationResponse>(
    actionName, transportService, actionFilters, ::BulkReplicationRequest
), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportBulkReplicationAction::class.java)
        private val completedTaskStatusMap = ConcurrentHashMap<String, BulkReplicationTaskStatus>()
        fun putCompletedStatus(taskId: String, status: BulkReplicationTaskStatus) { completedTaskStatusMap[taskId] = status }
        fun getCompletedStatus(taskId: String): BulkReplicationTaskStatus? = completedTaskStatusMap[taskId]
    }

    private val taskManager = transportService.taskManager

    // Checks cluster lock, resolves indices, spawns async task, returns task_id immediately.
    override fun doExecute(task: Task, request: BulkReplicationRequest, listener: ActionListener<BulkReplicationResponse>) {
        log.info("Received bulk ${operationType.label} request for pattern=${request.pattern}, excludeIndices=${request.excludeIndices.size}")

        val existing = clusterService.state().metadata
            .custom<BulkReplicationTaskMetadata>(BulkReplicationTaskMetadata.NAME)?.taskState
        if (existing != null && existing.numPending > 0) {
            log.warn("Rejected bulk ${operationType.label} request: another bulk task is already running (op=${existing.operationType}, pending=${existing.numPending})")
            listener.onFailure(OpenSearchStatusException(
                "A bulk replication task is already running. Only one bulk task is allowed at a time.", RestStatus.CONFLICT))
            return
        }

        launch(Dispatchers.Unconfined) {
            try {
                val (validIndices, preFailures) = resolveIndices(request)
                log.info("Index resolution for bulk ${operationType.label}: valid=${validIndices.size}, preFailures=${preFailures.size}")

                if (validIndices.isEmpty()) {
                    val msg = if (preFailures.isEmpty())
                        "No indices found matching pattern: [${request.pattern}]" + if (operationType == BulkOperationType.START) " on leader cluster" else ""
                    else
                        preFailures.joinToString("; ") { it.reason }
                    log.warn("No valid indices to process for bulk ${operationType.label}. Reason: ${msg.take(200)}")
                    listener.onFailure(if (preFailures.isEmpty()) ResourceNotFoundException(msg) else IllegalArgumentException(msg))
                    return@launch
                }
                spawnTask(operationType, replicationSettings.bulkBatchSize, request, listener, validIndices, preFailures)
            } catch (e: Exception) {
                log.error("Index resolution failed for bulk ${operationType.label}: ${e.message}", e)
                listener.onFailure(e)
            }
        }
    }

    // Routes to the correct resolver based on operation type.
    private suspend fun resolveIndices(request: BulkReplicationRequest): Pair<List<String>, List<FailedIndex>> {
        return when (operationType) {
            BulkOperationType.START -> resolveStartIndices(request)
            BulkOperationType.RESUME -> resolveResumeIndices(request)
            BulkOperationType.STOP, BulkOperationType.PAUSE -> resolveStopPauseIndices(request)
        }
    }

    // Resolves indices for START: validates alias, runs setup checks, fetches from leader, applies filters.
    private suspend fun resolveStartIndices(request: BulkReplicationRequest): Pair<List<String>, List<FailedIndex>> {
        val alias = request.leaderAlias
        if (alias.isNullOrBlank()) {
            val e = ActionRequestValidationException()
            e.addValidationError("leader_alias is required for bulk start")
            throw e
        }

        val user = SecurityContext.fromSecurityThreadContext(threadPool.threadContext)
        val followerClusterRole = request.useRoles?.get(ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE)
        val leaderClusterRole = request.useRoles?.get(ReplicateIndexRequest.LEADER_CLUSTER_ROLE)

        // Remote call: verify connection and permissions to leader cluster
        log.debug("Running setup checks for bulk_start, leader_alias=$alias")
        val setupChecksRes = client.suspendExecute(SetupChecksAction.INSTANCE, SetupChecksRequest(
            ReplicationContext(request.pattern, user?.overrideFgacRole(followerClusterRole)),
            ReplicationContext(request.pattern, user?.overrideFgacRole(leaderClusterRole)),
            alias
        ))
        if (!setupChecksRes.isAcknowledged) {
            throw IllegalStateException("Setup checks failed for leader cluster [$alias]")
        }

        // Remote call: fetch index list from leader matching the pattern
        log.debug("Fetching indices for bulk_start matching pattern=${request.pattern} from leader=$alias")
        val remoteClient = client.getRemoteClusterClient(alias)
        val indexReq = GetIndexRequest().features(*emptyArray())
            .indices(request.pattern)
            .indicesOptions(IndicesOptions.lenientExpandOpen())
        var leaderIndices = remoteClient.suspending(remoteClient.admin().indices()::getIndex, true)(indexReq).indices.toList()
        log.info("Leader returned ${leaderIndices.size} indices for bulk_start matching pattern=${request.pattern}")

        if (request.excludeIndices.isNotEmpty()) {
            leaderIndices = leaderIndices.filterNot { it in request.excludeIndices }
            log.info("After exclude filter for bulk_start: ${leaderIndices.size} indices remaining")
        }

        // Local check: which indices already exist on follower
        val followerState = clusterService.state()
        val preFailures = mutableListOf<FailedIndex>()
        val validIndices = mutableListOf<String>()
        for (index in leaderIndices) {
            if (followerState.routingTable.hasIndex(index))
                preFailures.add(FailedIndex(index, "Can't use same index again for replication. Delete the index:$index"))
            else
                validIndices.add(index)
        }
        if (preFailures.isNotEmpty()) {
            log.info("${preFailures.size} indices already exist on follower for bulk_start, ${validIndices.size} are new")
        }
        return validIndices to preFailures
    }

    // Resolves indices for STOP/PAUSE from the local follower cluster, marks invalid-state indices as failed.
    private suspend fun resolveStopPauseIndices(request: BulkReplicationRequest): Pair<List<String>, List<FailedIndex>> {
        val state = clusterService.state()
        val matchedIndices = resolveFollowerIndices(request, state)
        if (matchedIndices.isEmpty()) return emptyList<String>() to emptyList()

        val preFailures = mutableListOf<FailedIndex>()
        val validIndices = mutableListOf<String>()
        for (index in matchedIndices) {
            val reason = validateIndexForOperation(index)
            if (reason != null) preFailures.add(FailedIndex(index, reason)) else validIndices.add(index)
        }
        return validIndices to preFailures
    }

    // Resolves indices for RESUME: checks index exists and is paused, then verifies retention leases
    // on leader to fail-fast indices whose replication history has been discarded.
    private suspend fun resolveResumeIndices(request: BulkReplicationRequest): Pair<List<String>, List<FailedIndex>> {
        val (validAfterStateCheck, preFailures) = resolveStopPauseIndices(request)
        if (validAfterStateCheck.isEmpty()) return validAfterStateCheck to preFailures

        val state = clusterService.state()
        val finalFailures = preFailures.toMutableList()
        val finalValid = mutableListOf<String>()
        for (index in validAfterStateCheck) {
            if (hasRetentionLease(index, state)) finalValid.add(index)
            else finalFailures.add(FailedIndex(index, "Retention lease doesn't exist. Replication can't be resumed for $index"))
        }
        return finalValid to finalFailures
    }

    private fun resolveFollowerIndices(request: BulkReplicationRequest, state: ClusterState): List<String> {
        val matched = indexNameExpressionResolver.concreteIndexNames(
            state, IndicesOptions.lenientExpandOpen(), request.pattern
        ).toMutableList()
        if (request.excludeIndices.isNotEmpty()) {
            val excluded = indexNameExpressionResolver.concreteIndexNames(
                state, IndicesOptions.lenientExpandOpen(), *request.excludeIndices.toTypedArray()
            ).toSet()
            matched.removeAll(excluded)
        }
        return matched
    }

    /**
     * Checks whether a single index is in the right state for the requested operation.
     * Returns reason if the index cannot proceed, or null if it looks fine.
     * - STOP  : index must be actively replicating.
     * - PAUSE : index must be running (not already paused or stuck in an error state).
     * - RESUME: index must be paused.
     */
    private fun validateIndexForOperation(index: String): String? {
        val stateParams = getReplicationStateParamsForIndex(clusterService, index)
        return when (operationType) {
            BulkOperationType.STOP ->
                if (stateParams == null) "No replication in progress for index:$index" else null
            BulkOperationType.PAUSE -> when {
                stateParams == null -> "No replication in progress for index:$index"
                stateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE] == ReplicationOverallState.PAUSED.name -> "Index $index is already paused"
                stateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE] != ReplicationOverallState.RUNNING.name ->
                    "Cannot pause when in ${stateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE]} state"
                else -> null
            }
            BulkOperationType.RESUME -> when {
                stateParams == null -> "No replication in progress for index:$index"
                stateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE] != ReplicationOverallState.PAUSED.name -> "Replication on Index $index is already running"
                else -> null
            }
            else -> throw IllegalStateException("validateIndexForOperation should not be called for $operationType")
        }
    }

    private suspend fun hasRetentionLease(index: String, state: ClusterState): Boolean {
        return try {
            val replMetadata = replicationMetadataManager.getIndexReplicationMetadata(index)
            val remoteClient = client.getRemoteClusterClient(replMetadata.connectionName)
            val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(
                clusterService.clusterName.value(), state.metadata.clusterUUID(), remoteClient
            )
            val shards = state.routingTable.indicesRouting().get(index)?.shards() ?: return true
            val indexUUID = state.metadata.index(index)?.indexUUID ?: "_na_"
            shards.all { entry ->
                val followerShardId = entry.value.shardId
                retentionLeaseHelper.verifyRetentionLeaseExist(
                    ShardId(Index(index, indexUUID), followerShardId.id), followerShardId
                )
            }
        } catch (e: Exception) {
            log.warn("Failed to verify retention lease for index=$index, allowing resume attempt: ${e.message}")
            true
        }
    }

    /**
     * Clears the cluster-wide "a bulk task is running" flag so future bulk requests
     * are not permanently blocked. Must be called on every failure path after the flag
     * has been set. If clearing the flag itself fails, it is logged as an error because
     * it will require manual intervention to unblock the cluster.
     */
    internal suspend fun releaseLock() {
        for (attempt in 1..3) {
            try {
                client.suspendExecute(UpdateBulkTaskStateAction.INSTANCE, UpdateBulkTaskStateRequest(null))
                return
            } catch (e: Exception) {
                if (attempt == 3) {
                    log.error("Failed to release bulk task cluster state lock after $attempt attempts.", e)
                } else {
                    log.warn("Failed to release bulk task lock (attempt $attempt/3), retrying: ${e.message}")
                    kotlinx.coroutines.delay(1000)
                }
            }
        }
    }

    /**
     * Starts the bulk task: sets the "a task is running" flag in cluster state, registers
     * the task so it shows up in the tasks API, immediately tells the caller the task ID
     * so they can poll for progress without waiting for all indices to be processed, then
     * runs the actual work in the background.
     *
     * The "running" flag is written through the cluster manager so only one bulk task
     * can run at a time across the whole cluster — any concurrent attempt is rejected.
     *
     * If anything goes wrong after the flag is set, it is always cleared so the next
     * bulk request is not left permanently blocked.
     */
    private suspend fun spawnTask(
        opType: BulkOperationType, batchSize: Int, request: BulkReplicationRequest,
        listener: ActionListener<BulkReplicationResponse>,
        validIndices: List<String>, preFailedIndices: List<FailedIndex>
    ) {
        val claimState = BulkTaskState(
            taskId = "",
            operationType = opType.label,
            pattern = request.pattern,
            startTime = System.currentTimeMillis(),
            numSuccess = 0,
            numFailed = preFailedIndices.size,
            numPending = validIndices.size,
            numCancelled = 0,
            failedIndices = preFailedIndices
        )
        client.suspendExecute(UpdateBulkTaskStateAction.INSTANCE, UpdateBulkTaskStateRequest(claimState))

        val bulkTask = try {
            val taskAwareRequest = object : TaskAwareRequest {
                override fun setParentTask(parentTaskId: TaskId) {}
                override fun getParentTask(): TaskId = TaskId.EMPTY_TASK_ID
                override fun createTask(id: Long, type: String, action: String, parentTaskId: TaskId, headers: Map<String, String>): Task {
                    return BulkReplicationTask(
                        id = id, type = type, action = action,
                        description = "bulk ${opType.label} for pattern=${request.pattern}",
                        parentTaskId = parentTaskId, headers = headers,
                        request = request, operationType = opType,
                        client = client, clusterService = clusterService, threadPool = threadPool,
                        bulkBatchSize = batchSize,
                        bulkPollTimeoutMinutes = replicationSettings.bulkPollTimeout,
                        preResolvedIndices = validIndices,
                        preFailedIndices = preFailedIndices,
                        transportAction = this@TransportBulkReplicationAction
                    )
                }
            }
            taskManager.register(BulkReplicationTask::class.java.simpleName, actionName, taskAwareRequest) as BulkReplicationTask
        } catch (e: Exception) {
            log.error("Failed to register task for ${opType.label}, releasing cluster lock", e)
            releaseLock()
            listener.onFailure(e)
            return
        }
        val taskId = "${clusterService.localNode().id}:${bulkTask.id}"
        log.info("Spawned task for ${opType.label} taskId=$taskId pattern=${request.pattern}")

        try {
            listener.onResponse(BulkReplicationResponse(true, taskId))
        } catch (e: Exception) {
            log.error("Failed to send response for ${opType.label} taskId=$taskId: ${e.message}")
            taskManager.unregister(bulkTask)
            releaseLock()
            return
        }

        try {
            launch(threadPool.coroutineContext(ThreadPool.Names.GENERIC)) {
                bulkTask.run(object : ActionListener<BulkReplicationTaskStatus> {
                    override fun onResponse(status: BulkReplicationTaskStatus) {
                        log.info("Task completed for ${opType.label} taskId=$taskId: success=${status.numSuccess} failed=${status.numFailed}")
                        putCompletedStatus(taskId, status)
                        taskManager.unregister(bulkTask)
                    }
                    override fun onFailure(e: Exception) {
                        log.error("Task failed for ${opType.label} taskId=$taskId: ${e.message}")
                        putCompletedStatus(taskId, bulkTask.getStatus() as BulkReplicationTaskStatus)
                        taskManager.unregister(bulkTask)
                    }
                }, taskId)
            }
        } catch (e: Exception) {
            log.error("Failed to launch task for ${opType.label} taskId=$taskId: ${e.message}")
            putCompletedStatus(taskId, bulkTask.getStatus() as BulkReplicationTaskStatus)
            taskManager.unregister(bulkTask)
            releaseLock()
        }
    }


    // Starts replication for a batch: 4 shared remote calls to leader (setup, cluster state, settings, metadata),
    // local validation loops, then parallel metadata writes + task starts. Rollbacks metadata if task start fails.
    internal suspend fun executeBatchStart(
        indices: List<String>,
        request: BulkReplicationRequest
    ): Pair<List<String>, List<FailedIndex>> {
        if (indices.isEmpty()) return emptyList<String>() to emptyList()
        log.debug("Processing batch of ${indices.size} indices for bulk_start")

        val failures = CopyOnWriteArrayList<FailedIndex>()
        var remaining = indices.toMutableList()
        val alias = request.leaderAlias!!
        val user = SecurityContext.fromSecurityThreadContext(threadPool.threadContext)
        val followerRole = request.useRoles?.get(ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE)
        val leaderRole = request.useRoles?.get(ReplicateIndexRequest.LEADER_CLUSTER_ROLE)

        val leaderState = try {
            log.debug("Fetching leader cluster state for bulk_start for ${remaining.size} indices")
            replicateIndexAction.getLeaderClusterState(alias, remaining)
        } catch (e: Exception) {
            remaining.forEach { failures.add(FailedIndex(it, "Failed to fetch leader cluster state: ${e.message}")) }
            log.debug("Failed to fetch leader cluster state for bulk_start from $alias: ${e.message}")
            return emptyList<String>() to failures.toList()
        }

        remaining = remaining.filter { index ->
            try {
                ValidationUtil.validateLeaderIndexState(alias, index, leaderState)
                true
            } catch (e: Exception) {
                failures.add(FailedIndex(index, e.message ?: "Leader index validation failed"))
                log.debug("Leader index validation failed for bulk_start for index=$index: ${e.message}")
                false
            }
        }.toMutableList()
        if (remaining.isEmpty()) return emptyList<String>() to failures.toList()

        val settingsMap = try {
            log.debug("Fetching settings for bulk_start for ${remaining.size} indices from leader=$alias")
            replicateIndexAction.getLeaderIndexSettings(alias, remaining)
        } catch (e: Exception) {
            remaining.forEach { failures.add(FailedIndex(it, "Failed to fetch leader settings: ${e.message}")) }
            log.debug("Failed to fetch leader settings for bulk_start from $alias: ${e.message}")
            return emptyList<String>() to failures.toList()
        }

        remaining = remaining.filter { index ->
            val settings = settingsMap[index]
            if (settings == null || settings.isEmpty) {
                failures.add(FailedIndex(index, "Index not found on leader: $alias:$index"))
                return@filter false
            }
            if (settings.keySet().contains(ReplicationPlugin.REPLICATED_INDEX_SETTING.key) &&
                !settings.get(ReplicationPlugin.REPLICATED_INDEX_SETTING.key).isNullOrBlank()) {
                failures.add(FailedIndex(index, "Cannot replicate a replicated index: $index"))
                return@filter false
            }
            if (!settings.getAsBoolean("index.soft_deletes.enabled", true)) {
                failures.add(FailedIndex(index, "Cannot replicate index where soft_deletes is disabled: $index"))
                return@filter false
            }
            true
        }.toMutableList()
        if (remaining.isEmpty()) return emptyList<String>() to failures.toList()

        if (clusterService.clusterSettings.get(ReplicationPlugin.REPLICATION_FOLLOWER_BLOCK_START)) {
            remaining.forEach { failures.add(FailedIndex(it, "Replication START block is set")) }
            log.debug("Replication start block is active for bulk_start, failing ${remaining.size} indices")
            return emptyList<String>() to failures.toList()
        }

        val metadataMap = try {
            log.debug("Fetching full metadata for bulk_start for ${remaining.size} indices from leader=$alias")
            clusterManagerAction.getRemoteIndexMetadata(alias, remaining)
        } catch (e: Exception) {
            remaining.forEach { failures.add(FailedIndex(it, "Failed to fetch remote index metadata: ${e.message}")) }
            log.debug("Failed to fetch remote metadata for bulk_start from $alias: ${e.message}")
            return emptyList<String>() to failures.toList()
        }

        val state = clusterService.state()
        remaining = remaining.filter { index ->
            if (state.routingTable.hasIndex(index)) {
                failures.add(FailedIndex(index, "Cant use same index again for replication. Delete the index:$index"))
                false
            } else true
        }.toMutableList()
        if (remaining.isEmpty()) return emptyList<String>() to failures.toList()

        coroutineScope {
            remaining.map { index ->
                async {
                    try {
                        StaleTaskUtils.removeAllTasksForIndex(clusterService, client, index)
                    } catch (e: Exception) {
                        log.debug("Failed to clean stale tasks for bulk_start for index=$index (non-fatal): ${e.message}")
                    }
                }
            }.awaitAll()
        }

        log.debug("Starting parallel writes for bulk_start for ${remaining.size} indices")
        val succeeded = CopyOnWriteArrayList<String>()
        var toAttempt = remaining.toList()

        for (attempt in 1..3) {
            val failedThisAttempt = CopyOnWriteArrayList<FailedIndex>()
            coroutineScope {
                toAttempt.map { index ->
                    async {
                        try {
                            val metadata = metadataMap[index]
                                ?: throw IllegalStateException("No metadata found for leader index: $index")

                            replicationMetadataManager.addIndexReplicationMetadata(
                                index, alias, index, ReplicationOverallState.RUNNING,
                                user, followerRole, leaderRole, Settings.EMPTY
                            )

                            try {
                                val params = IndexReplicationParams(alias, metadata.index, index)
                                persistentTasksService.startTask(
                                    "replication:index:$index", IndexReplicationExecutor.TASK_NAME, params
                                )
                            } catch (e: Exception) {
                                try { replicationMetadataManager.deleteIndexReplicationMetadata(index) } catch (e: Exception) { log.debug("Rollback: failed to delete metadata for index=$index: ${e.message}") }
                                throw e
                            }

                            succeeded.add(index)
                            log.debug("Successfully initiated replication for bulk_start for index=$index")
                        } catch (e: Exception) {
                            log.debug("Failed to start replication for bulk_start for index=$index (attempt $attempt/3): ${e.message}")
                            failedThisAttempt.add(FailedIndex(index, e.message ?: "unknown error"))
                        }
                    }
                }.awaitAll()
            }

            toAttempt = failedThisAttempt.map { it.index }
            if (toAttempt.isEmpty()) break
            if (attempt < 3) kotlinx.coroutines.delay(2000)
            else failedThisAttempt.forEach { failures.add(it) }
        }
        log.debug("Batch complete for bulk_start: ${succeeded.size} initiated, ${failures.size - (indices.size - remaining.size)} failed")

        return succeeded.toList() to failures.toList()
    }

    // Pauses a batch: checks metadata write block + restore state (local), then updates state to PAUSED in parallel.
    internal suspend fun executeBatchPause(
        indices: List<String>,
        reason: String = "bulk_pause"
    ): Pair<List<String>, List<FailedIndex>> {
        if (indices.isEmpty()) return emptyList<String>() to emptyList()
        log.debug("Processing batch of ${indices.size} indices for bulk_pause")

        val failures = CopyOnWriteArrayList<FailedIndex>()

        val state = clusterService.state()
        val blockException = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
        if (blockException != null) {
            indices.forEach { failures.add(FailedIndex(it, "Cluster metadata write block is active: ${blockException.message}")) }
            log.debug("Metadata write block is active for bulk_pause, failing entire batch")
            return emptyList<String>() to failures.toList()
        }

        val restoreInProgress = state.custom<RestoreInProgress>(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)
        val restoringIndices = restoreInProgress.flatMap { it.indices() }.toSet()

        val remaining = indices.filter { index ->
            if (index in restoringIndices) {
                failures.add(FailedIndex(index, "Index is in restore phase. You can pause after restore completes."))
                log.debug("Skipping index=$index for bulk_pause: currently restoring")
                false
            } else true
        }
        if (remaining.isEmpty()) return emptyList<String>() to failures.toList()

        log.debug("Updating state to PAUSED for bulk_pause for ${remaining.size} indices in parallel")
        val succeeded = CopyOnWriteArrayList<String>()
        var toAttempt = remaining.toList()

        for (attempt in 1..3) {
            val failedThisAttempt = CopyOnWriteArrayList<FailedIndex>()
            coroutineScope {
                toAttempt.map { index ->
                    async {
                        try {
                            replicationMetadataManager.updateIndexReplicationState(index, ReplicationOverallState.PAUSED, reason)
                            succeeded.add(index)
                        } catch (e: Exception) {
                            log.debug("Failed to pause for bulk_pause for index=$index (attempt $attempt/3): ${e.message}")
                            failedThisAttempt.add(FailedIndex(index, e.message ?: "Failed to pause"))
                        }
                    }
                }.awaitAll()
            }

            toAttempt = failedThisAttempt.map { it.index }
            if (toAttempt.isEmpty()) break
            if (attempt < 3) kotlinx.coroutines.delay(2000)
            else failedThisAttempt.forEach { failures.add(it) }
        }
        log.debug("Batch complete for bulk_pause: ${succeeded.size} paused, ${failures.size} failed")

        return succeeded.toList() to failures.toList()
    }

    // Resumes a batch: 1 shared remote call per leader alias for metadata, parallel per-index retention lease checks
    // (remote), then parallel state updates + task starts. Rollbacks state to PAUSED if task start fails.
    internal suspend fun executeBatchResume(
        indices: List<String>
    ): Pair<List<String>, List<FailedIndex>> {
        if (indices.isEmpty()) return emptyList<String>() to emptyList()
        log.debug("Processing batch of ${indices.size} indices for bulk_resume")

        val failures = CopyOnWriteArrayList<FailedIndex>()

        val blockException = clusterService.state().blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
        if (blockException != null) {
            indices.forEach { failures.add(FailedIndex(it, "Cluster metadata write block is active: ${blockException.message}")) }
            log.debug("Metadata write block is active for bulk_resume, failing entire batch")
            return emptyList<String>() to failures.toList()
        }

        data class ResumeCtx(val index: String, val alias: String, val leaderIndex: String)
        val contexts = mutableListOf<ResumeCtx>()
        val metadataByIndex = try {
            replicationMetadataManager.getIndexReplicationMetadata(indices)
        } catch (e: Exception) {
            indices.forEach { failures.add(FailedIndex(it, "Failed to fetch replication metadata: ${e.message}")) }
            log.debug("Failed to fetch replication metadata for bulk_resume: ${e.message}")
            return emptyList<String>() to failures.toList()
        }
        for (index in indices) {
            val md = metadataByIndex[index]
            if (md == null) {
                failures.add(FailedIndex(index, "No replication metadata found for index: $index"))
                log.debug("No replication metadata found for bulk_resume for index=$index")
            } else {
                contexts.add(ResumeCtx(index, md.connectionName, md.leaderContext.resource))
            }
        }
        if (contexts.isEmpty()) return emptyList<String>() to failures.toList()

        val metadataMap = mutableMapOf<String, IndexMetadata>()
        for ((alias, group) in contexts.groupBy { it.alias }) {
            try {
                log.debug("Fetching metadata for bulk_resume from leader=$alias for ${group.size} indices")
                val leaderIndices = group.map { it.leaderIndex }
                val map = clusterManagerAction.getRemoteIndexMetadata(alias, leaderIndices)
                for (ctx in group) {
                    map[ctx.leaderIndex]?.let { metadataMap[ctx.index] = it }
                }
            } catch (e: Exception) {
                log.debug("Failed to fetch leader metadata for bulk_resume from alias=$alias: ${e.message}")
                group.forEach { failures.add(FailedIndex(it.index, "Failed to fetch leader metadata: ${e.message}")) }
            }
        }

        val remaining = contexts.filter { metadataMap.containsKey(it.index) }
        if (remaining.isEmpty()) return emptyList<String>() to failures.toList()

        log.debug("Checking retention leases for bulk_resume for ${remaining.size} indices")
        val validAfterLease = CopyOnWriteArrayList<ResumeCtx>()
        coroutineScope {
            remaining.map { ctx ->
                async {
                    try {
                        val metadata = metadataMap[ctx.index]!!
                        val params = IndexReplicationParams(ctx.alias, metadata.index, ctx.index)
                        if (hasRetentionLease(params)) {
                            validAfterLease.add(ctx)
                        } else {
                            failures.add(FailedIndex(ctx.index, "Retention lease doesn't exist. Replication can't be resumed for ${ctx.index}"))
                            log.debug("Retention lease expired for bulk_resume for index=${ctx.index}")
                        }
                    } catch (e: Exception) {
                        log.debug("Lease check failed for bulk_resume for index=${ctx.index}, allowing resume attempt: ${e.message}")
                        validAfterLease.add(ctx)
                    }
                }
            }.awaitAll()
        }
        if (validAfterLease.isEmpty()) return emptyList<String>() to failures.toList()

        log.debug("Starting parallel writes for bulk_resume for ${validAfterLease.size} indices")
        val succeeded = CopyOnWriteArrayList<String>()
        var toAttempt = validAfterLease.toList()

        for (attempt in 1..3) {
            val failedThisAttempt = CopyOnWriteArrayList<Pair<ResumeCtx, String>>()
            coroutineScope {
                toAttempt.map { ctx ->
                    async {
                        try {
                            StaleTaskUtils.removeAllTasksForIndex(clusterService, client, ctx.index)
                            replicationMetadataManager.updateIndexReplicationState(ctx.index, ReplicationOverallState.RUNNING)

                            try {
                                val metadata = metadataMap[ctx.index]!!
                                val params = IndexReplicationParams(ctx.alias, metadata.index, ctx.index)
                                persistentTasksService.startTask(
                                    "replication:index:${ctx.index}", IndexReplicationExecutor.TASK_NAME, params
                                )
                            } catch (e: Exception) {
                                try { replicationMetadataManager.updateIndexReplicationState(ctx.index, ReplicationOverallState.PAUSED, "rollback: task start failed") } catch (e: Exception) { log.debug("Rollback: failed to reset state to PAUSED for index=${ctx.index}: ${e.message}") }
                                throw e
                            }

                            succeeded.add(ctx.index)
                            log.debug("Successfully resumed replication for bulk_resume for index=${ctx.index}")
                        } catch (e: Exception) {
                            log.debug("Failed to resume for bulk_resume for index=${ctx.index} (attempt $attempt/3): ${e.message}")
                            failedThisAttempt.add(ctx to (e.message ?: "unknown error"))
                        }
                    }
                }.awaitAll()
            }

            toAttempt = failedThisAttempt.map { it.first }
            if (toAttempt.isEmpty()) break
            if (attempt < 3) kotlinx.coroutines.delay(2000)
            else failedThisAttempt.forEach { (ctx, reason) -> failures.add(FailedIndex(ctx.index, reason)) }
        }
        log.debug("Batch complete for bulk_resume: ${succeeded.size} resumed, ${failures.size} failed")

        return succeeded.toList() to failures.toList()
    }

    // Stops replication for a batch: checks metadata write block (local), then 7 sequential phases
    // each parallel across all indices. Step 4 uses the single-index stop action to route the
    // cluster state update to cluster-manager (steps 1-3 pre-work makes it fast, steps 5-7 are no-ops inside it).
    internal suspend fun executeBatchStop(
        indices: List<String>
    ): Pair<List<String>, List<FailedIndex>> {
        if (indices.isEmpty()) return emptyList<String>() to emptyList()
        val blockException = clusterService.state().blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
        if (blockException != null) {
            return emptyList<String>() to indices.map { FailedIndex(it, "Cluster metadata write block is active: ${blockException.message}") }
        }
        val succeeded = CopyOnWriteArrayList<String>()
        val failures = CopyOnWriteArrayList<FailedIndex>()
        val failedSet = ConcurrentHashMap.newKeySet<String>()

        coroutineScope { indices.map { index -> async {
            try { client.suspendExecute(UpdateIndexBlockAction.INSTANCE,
                UpdateIndexBlockRequest(index, IndexBlockUpdateType.REMOVE_BLOCK), injectSecurityContext = true)
            } catch (e: Exception) { log.debug("Failed to remove block for index=$index: ${e.message}") }
        }}.awaitAll() }

        val restoringIndices = clusterService.state()
            .custom<RestoreInProgress>(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)
            .flatMap { it.indices() }.toSet()
        val closeable = indices.filter { it !in restoringIndices && clusterService.state().routingTable.hasIndex(it) }
        coroutineScope { closeable.map { index -> async {
            try { client.suspendExecute(UpdateMetadataAction.INSTANCE,
                UpdateMetadataRequest(index, UpdateMetadataRequest.Type.CLOSE, Requests.closeIndexRequest(index)),
                injectSecurityContext = true)
            } catch (e: Exception) { log.debug("Failed to close index=$index: ${e.message}") }
        }}.awaitAll() }

        coroutineScope { indices.map { index -> async {
            try {
                val replMetadata = replicationMetadataManager.getIndexReplicationMetadata(index)
                val remoteClient = client.getRemoteClusterClient(replMetadata.connectionName)
                RemoteClusterRetentionLeaseHelper(
                    clusterService.clusterName.value(), clusterService.state().metadata.clusterUUID(), remoteClient
                ).attemptRemoveRetentionLease(clusterService, replMetadata, index)
            } catch (e: Exception) { log.debug("Failed to remove retention lease for index=$index: ${e.message}") }
        }}.awaitAll() }

        val semaphore = kotlinx.coroutines.sync.Semaphore(20)
        coroutineScope { indices.map { index -> async {
            semaphore.acquire()
            try {
                val res = client.suspendExecute(
                    org.opensearch.commons.replication.action.ReplicationActions.STOP_REPLICATION_ACTION_TYPE,
                    StopIndexReplicationRequest(index),
                    injectSecurityContext = true
                )
                if (res.isAcknowledged) succeeded.add(index)
                else { failures.add(FailedIndex(index, "Stop not acknowledged")); failedSet.add(index) }
            } catch (e: Exception) {
                failures.add(FailedIndex(index, e.message ?: "Stop failed"))
                failedSet.add(index)
            } finally { semaphore.release() }
        }}.awaitAll() }

        val stopped = indices.filter { it !in failedSet }

        val reopenable = closeable.filter { it in stopped && clusterService.state().routingTable.hasIndex(it) }
        coroutineScope { reopenable.map { index -> async {
            try { client.suspending(client.admin().indices()::open, injectSecurityContext = true)(OpenIndexRequest(index))
            } catch (e: Exception) { log.debug("Failed to reopen index=$index: ${e.message}") }
        }}.awaitAll() }

        coroutineScope { stopped.map { index -> async {
            try { StaleTaskUtils.removeAllTasksForIndex(clusterService, client, index)
            } catch (e: Exception) { log.debug("Failed to remove stale tasks for index=$index: ${e.message}") }
        }}.awaitAll() }

        coroutineScope { stopped.map { index -> async {
            try { replicationMetadataManager.deleteIndexReplicationMetadata(index)
            } catch (e: Exception) { log.debug("Failed to delete metadata for index=$index: ${e.message}") }
        }}.awaitAll() }

        return succeeded.toList() to failures.toList()
    }

    private suspend fun hasRetentionLease(params: IndexReplicationParams): Boolean {
        return try {
            val remoteClient = client.getRemoteClusterClient(params.leaderAlias)
            val shards = clusterService.state().routingTable.indicesRouting().get(params.followerIndexName)?.shards()
                ?: return true
            val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(
                clusterService.clusterName.value(), clusterService.state().metadata.clusterUUID(), remoteClient
            )
            shards.entries.all { entry ->
                val followerShardId = entry.value.shardId
                retentionLeaseHelper.verifyRetentionLeaseExist(
                    ShardId(params.leaderIndex, followerShardId.id), followerShardId
                )
            }
        } catch (e: Exception) {
            log.warn("Failed to verify retention lease for ${params.followerIndexName}: ${e.message}")
            true
        }
    }
}