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

package org.opensearch.replication.task.bulk

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.opensearch.replication.util.coroutineContext
import org.apache.logging.log4j.LogManager
import org.opensearch.cluster.service.ClusterService
import org.opensearch.commons.replication.action.ReplicationActions.STOP_REPLICATION_ACTION_TYPE
import org.opensearch.commons.replication.action.StopIndexReplicationRequest
import org.opensearch.core.action.ActionListener
import org.opensearch.core.tasks.TaskId
import org.opensearch.replication.action.bulk.BulkReplicationRequest
import org.opensearch.replication.action.bulk.UpdateBulkTaskStateAction
import org.opensearch.replication.action.bulk.UpdateBulkTaskStateRequest
import org.opensearch.replication.action.index.ReplicateIndexAction
import org.opensearch.replication.action.index.ReplicateIndexRequest
import org.opensearch.replication.action.pause.PauseIndexReplicationAction
import org.opensearch.replication.action.pause.PauseIndexReplicationRequest
import org.opensearch.replication.action.resume.ResumeIndexReplicationAction
import org.opensearch.replication.action.resume.ResumeIndexReplicationRequest
import org.opensearch.replication.metadata.state.BulkTaskState
import org.opensearch.replication.util.suspendExecute
import org.opensearch.tasks.CancellableTask
import org.opensearch.tasks.Task
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

enum class BulkOperationType(val label: String) {
    START("bulk_start_replication"),
    STOP("bulk_stop_replication"),
    PAUSE("bulk_pause_replication"),
    RESUME("bulk_resume_replication")
}

class BulkReplicationTask(
    id: Long,
    type: String,
    action: String,
    description: String,
    parentTaskId: TaskId,
    headers: Map<String, String>,
    private val request: BulkReplicationRequest,
    private val operationType: BulkOperationType,
    private val client: Client,
    private val clusterService: ClusterService,
    private val threadPool: ThreadPool,
    private val bulkBatchSize: Int,
    private val preResolvedIndices: List<String>,
    private val preFailedIndices: List<FailedIndex>
) : CancellableTask(id, type, action, description, parentTaskId, headers) {

    companion object {
        private val log = LogManager.getLogger(BulkReplicationTask::class.java)
    }

    private val startTime = System.currentTimeMillis()
    private val successCount = AtomicInteger(0)
    private val failedIndices = CopyOnWriteArrayList<FailedIndex>(preFailedIndices)
    private var bulkTaskId: String = ""
    private val pendingCount = AtomicInteger(preResolvedIndices.size)
    private val cancelledCount = AtomicInteger(0)

    override fun shouldCancelChildrenOnCancellation() = false

    override fun getStatus(): Task.Status = BulkReplicationTaskStatus(
        operationType = operationType.label,
        pattern = request.pattern,
        startTime = startTime,
        numSuccess = successCount.get(),
        numFailed = failedIndices.size,
        numPending = pendingCount.get(),
        numCancelled = cancelledCount.get(),
        failedIndices = failedIndices.toList()
    )

    /**
     * Runs the bulk operation across all pre-resolved indices.
     * Processes them in batches so we don't overload the cluster and can write
     * progress updates between batches. Checks for cancellation before each batch
     * so the task stops promptly when cancelled rather than running to completion.
     * Responds to the caller before writing the final progress update to cluster
     * state, so the caller gets the result without waiting for that write.
     */
    fun run(listener: ActionListener<BulkReplicationTaskStatus>, taskId: String) {
        bulkTaskId = taskId
        CoroutineScope(threadPool.coroutineContext(ThreadPool.Names.GENERIC)).launch {
            try {
                val indices = preResolvedIndices
                updateClusterState()
                log.info("BulkReplicationTask[$operationType] processing ${indices.size} indices for pattern=${request.pattern}")

                for (batch in indices.chunked(bulkBatchSize)) {
                    if (isCancelled) {
                        log.info("BulkReplicationTask[$operationType] cancelled")
                        break
                    }
                    processBatch(batch)
                    updateClusterState()
                }

                if (isCancelled) {
                    val newFailures = failedIndices.size - preFailedIndices.size
                    cancelledCount.set(maxOf(0, indices.size - successCount.get() - newFailures))
                }
                pendingCount.set(0)
                listener.onResponse(getStatus() as BulkReplicationTaskStatus)
                updateClusterState()
            } catch (e: Exception) {
                log.error("BulkReplicationTask[$operationType] failed", e)
                pendingCount.set(0)
                listener.onFailure(e)
                updateClusterState()
            }
        }
    }

    /**
     * Persists task progress counters to cluster state so any node can serve
     * status queries. Write failures are swallowed since the final state is
     * always flushed before the task completes.
     */
    private suspend fun updateClusterState() {
        val state = BulkTaskState(
            taskId = bulkTaskId,
            operationType = operationType.label,
            pattern = request.pattern,
            startTime = startTime,
            numSuccess = successCount.get(),
            numFailed = failedIndices.size,
            numPending = pendingCount.get(),
            numCancelled = cancelledCount.get(),
            failedIndices = failedIndices.toList()
        )
        try {
            client.suspendExecute(UpdateBulkTaskStateAction.INSTANCE, UpdateBulkTaskStateRequest(state))
        } catch (e: Exception) {
            log.warn("Failed to update bulk task state in cluster state: ${e.message}", e)
        }
    }

    /**
     * Executes the bulk operation for a single batch of indices sequentially.
     * Each index is attempted independently; a failure on one index is recorded in
     * [failedIndices] and does not abort the remaining indices in the batch.
     * Checks [isCancelled] before each index so in-progress batches can be interrupted
     * mid-way without waiting for the full batch to complete.
     * [pendingCount] is decremented in the finally block to keep the live status accurate
     * regardless of whether the per-index operation succeeded or failed.
     */
    private suspend fun processBatch(batch: List<String>) {
        for (index in batch) {
            if (isCancelled) return
            try {
                executeOnIndex(index)
                successCount.incrementAndGet()
                log.debug("BulkReplicationTask[$operationType] succeeded for index=$index")
            } catch (e: Exception) {
                log.warn("BulkReplicationTask[$operationType] failed for index=$index: ${e.message}")
                failedIndices.add(FailedIndex(index, e.message ?: "unknown error"))
            } finally {
                pendingCount.updateAndGet { maxOf(0, it - 1) }
            }
        }
    }

    
    /**
     * Runs the operation on a single index by delegating to the existing
     * per-index transport action (stop, pause, resume, or start).
     * This reuses all the validation, security checks, and error handling
     * already defined in each of those actions. 
     * For START, the follower index is given the same name as the leader index.
     */
    private suspend fun executeOnIndex(index: String) {
        when (operationType) {
            BulkOperationType.STOP ->
                client.suspendExecute(STOP_REPLICATION_ACTION_TYPE, StopIndexReplicationRequest(index), injectSecurityContext = true)
            BulkOperationType.PAUSE ->
                client.suspendExecute(PauseIndexReplicationAction.INSTANCE, PauseIndexReplicationRequest(index, "bulk_pause"), injectSecurityContext = true)
            BulkOperationType.RESUME ->
                client.suspendExecute(ResumeIndexReplicationAction.INSTANCE, ResumeIndexReplicationRequest(index), injectSecurityContext = true)
            BulkOperationType.START -> {
                val alias = request.leaderAlias
                    ?: throw IllegalArgumentException("leader_alias is required for bulk start")
                val req = ReplicateIndexRequest(index, alias, index).also {
                    it.isAutoFollowRequest = true
                    it.useRoles = request.useRoles
                }
                client.suspendExecute(ReplicateIndexAction.INSTANCE, req, injectSecurityContext = true)
            }
        }
    }
}
