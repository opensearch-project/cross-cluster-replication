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
import kotlinx.coroutines.launch
import org.opensearch.replication.util.coroutineContext
import org.apache.logging.log4j.LogManager
import org.opensearch.ResourceAlreadyExistsException
import org.opensearch.ResourceNotFoundException
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
import org.opensearch.tasks.Task
import org.opensearch.tasks.TaskAwareRequest
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import java.util.concurrent.ConcurrentHashMap

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
    private val replicationSettings: ReplicationSettings
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

    /**
     * Handles an incoming bulk replication request (start, stop, pause, or resume).
     * Does a quick local check first to reject early if another bulk task is already
     * running.
     * Finds which indices match the pattern and which ones can't proceed, then kicks
     * off the background task.
     */
    override fun doExecute(task: Task, request: BulkReplicationRequest, listener: ActionListener<BulkReplicationResponse>) {
        val existing = clusterService.state().metadata
            .custom<BulkReplicationTaskMetadata>(BulkReplicationTaskMetadata.NAME)?.taskState
        if (existing != null && existing.numPending > 0) {
            listener.onFailure(ResourceAlreadyExistsException(
                "A bulk replication task is already running. Only one bulk task is allowed at a time."))
            return
        }

        launch(Dispatchers.Unconfined) {
            try {
                val (validIndices, preFailures) = resolveIndices(request)
                if (validIndices.isEmpty()) {
                    val msg = if (preFailures.isEmpty())
                        "No indices found matching pattern: [${request.pattern}]" + if (operationType == BulkOperationType.START) " on leader cluster" else ""
                    else
                        preFailures.joinToString("; ") { it.reason }
                    listener.onFailure(if (preFailures.isEmpty()) ResourceNotFoundException(msg) else IllegalArgumentException(msg))
                    return@launch
                }
                spawnTask(operationType, replicationSettings.bulkBatchSize, request, listener, validIndices, preFailures)
            } catch (e: Exception) {
                listener.onFailure(e)
            }
        }
    }

    /**
     * Decides which indices to operate on based on the type of operation.
     * START asks the remote leader cluster for matching indices.
     * STOP and PAUSE look at the local follower cluster.
     * RESUME does the same as STOP/PAUSE but also checks whether the leader
     * still holds the history needed to safely pick up from where replication left off.
     */
    private suspend fun resolveIndices(request: BulkReplicationRequest): Pair<List<String>, List<FailedIndex>> {
        return when (operationType) {
            BulkOperationType.START -> resolveStartIndices(request)
            BulkOperationType.RESUME -> resolveResumeIndices(request)
            else -> resolveStopPauseIndices(request)
        }
    }

    /**
     * Finds the indices to start replicating by asking the remote leader cluster.
     * Verifies the connection and permissions before fetching the index list.
     * If an index with the same name already exists on the follower, it is marked
     * as failed with a clear message , the user needs to delete it first before
     * replication can be set up for that index.
     */
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

        val setupChecksRes = client.suspendExecute(SetupChecksAction.INSTANCE, SetupChecksRequest(
            ReplicationContext(request.pattern, user?.overrideFgacRole(followerClusterRole)),
            ReplicationContext(request.pattern, user?.overrideFgacRole(leaderClusterRole)),
            alias
        ))
        if (!setupChecksRes.isAcknowledged) {
            throw IllegalStateException("Setup checks failed for leader cluster [$alias]")
        }

        val remoteClient = client.getRemoteClusterClient(alias)
        val indexReq = GetIndexRequest().features(*emptyArray())
            .indices(request.pattern)
            .indicesOptions(IndicesOptions.lenientExpandOpen())
        var leaderIndices = remoteClient.suspending(remoteClient.admin().indices()::getIndex, true)(indexReq).indices.toList()
        if (request.excludeIndices.isNotEmpty()) leaderIndices = leaderIndices.filterNot { it in request.excludeIndices }

        val followerState = clusterService.state()
        val preFailures = mutableListOf<FailedIndex>()
        val validIndices = mutableListOf<String>()
        for (index in leaderIndices) {
            if (followerState.routingTable.hasIndex(index))
                preFailures.add(FailedIndex(index, "Can't use same index again for replication. Delete the index:$index"))
            else
                validIndices.add(index)
        }
        return validIndices to preFailures
    }

    /**
     * Finds indices eligible for STOP or PAUSE by checking the local follower cluster state.
     * Any index that is already in the wrong state — for example already paused, or not
     * replicating at all — is marked as failed with a clear reason rather than silently skipped,
     * so the user knows exactly why each index was not processed.
     */
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

    /**
     * Finds indices eligible for RESUME.
     * First checks that each index exists and is currently paused (same checks as STOP/PAUSE).
     * Then verifies that the leader still holds the replication history needed to pick up
     * from the last saved position — if the leader has already discarded that history,
     * resuming could lead to data gaps.
     * If the history check itself fails (e.g. the remote cluster is temporarily unreachable),
     * the index is allowed through so the actual resume attempt can report a clearer error.
     */
    private suspend fun resolveResumeIndices(request: BulkReplicationRequest): Pair<List<String>, List<FailedIndex>> {
        val (validAfterStateCheck, preFailures) = resolveStopPauseIndices(request)
        if (validAfterStateCheck.isEmpty()) return validAfterStateCheck to preFailures

        // Additional retention lease check for resume
        val state = clusterService.state()
        val finalFailures = preFailures.toMutableList()
        val finalValid = mutableListOf<String>()
        for (index in validAfterStateCheck) {
            if (hasRetentionLease(index, state)) finalValid.add(index)
            else finalFailures.add(FailedIndex(index, "Retention lease doesn't exist. Replication can't be resumed for $index"))
        }
        return finalValid to finalFailures
    }

    private fun resolveFollowerIndices(request: BulkReplicationRequest, state: org.opensearch.cluster.ClusterState): List<String> {
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

    /**
     * Checks whether the leader cluster still holds the replication history for every
     * shard of the given follower index. Without this history the leader may have already
     * deleted the operations needed to resume from the last saved position, making it
     * unsafe to resume.
     * If the check itself throws (e.g. the remote cluster is temporarily unreachable),
     * the index is allowed through so the actual resume attempt can surface the real error.
     */
    private suspend fun hasRetentionLease(index: String, state: org.opensearch.cluster.ClusterState): Boolean {
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
            true // on error, allow resume attempt
        }
    }

    /**
     * Clears the cluster-wide "a bulk task is running" flag so future bulk requests
     * are not permanently blocked. Must be called on every failure path after the flag
     * has been set. If clearing the flag itself fails, it is logged as an error because
     * it will require manual intervention to unblock the cluster.
     */
    private suspend fun releaseLock() {
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
                        preResolvedIndices = validIndices,
                        preFailedIndices = preFailedIndices
                    )
                }
            }
            taskManager.register(BulkReplicationTask::class.java.simpleName, actionName, taskAwareRequest) as BulkReplicationTask
        } catch (e: Exception) {
            log.error("Failed to register BulkReplicationTask[${opType.label}], releasing cluster lock", e)
            releaseLock()
            listener.onFailure(e)
            return
        }
        val taskId = "${clusterService.localNode().id}:${bulkTask.id}"
        log.info("Spawned BulkReplicationTask[$opType] taskId=$taskId pattern=${request.pattern}")

        try {
            listener.onResponse(BulkReplicationResponse(true, taskId))
        } catch (e: Exception) {
            log.error("BulkReplicationTask[$opType] taskId=$taskId failed to send response: ${e.message}")
            taskManager.unregister(bulkTask)
            releaseLock()
            return
        }

        try {
            launch(threadPool.coroutineContext(ThreadPool.Names.GENERIC)) {
                bulkTask.run(object : ActionListener<BulkReplicationTaskStatus> {
                    override fun onResponse(status: BulkReplicationTaskStatus) {
                        log.info("BulkReplicationTask[$opType] taskId=$taskId completed: success=${status.numSuccess} failed=${status.numFailed}")
                        putCompletedStatus(taskId, status)
                        taskManager.unregister(bulkTask)
                        launch(threadPool.coroutineContext(ThreadPool.Names.GENERIC)) { releaseLock() }
                    }
                    override fun onFailure(e: Exception) {
                        log.error("BulkReplicationTask[$opType] taskId=$taskId failed: ${e.message}")
                        putCompletedStatus(taskId, bulkTask.getStatus() as BulkReplicationTaskStatus)
                        taskManager.unregister(bulkTask)
                        launch(threadPool.coroutineContext(ThreadPool.Names.GENERIC)) { releaseLock() }
                    }
                }, taskId)
            }
        } catch (e: Exception) {
            log.error("BulkReplicationTask[$opType] taskId=$taskId failed to launch: ${e.message}")
            putCompletedStatus(taskId, bulkTask.getStatus() as BulkReplicationTaskStatus)
            taskManager.unregister(bulkTask)
            releaseLock()
        }
    }
}
