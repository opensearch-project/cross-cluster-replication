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
import org.opensearch.core.action.ActionListener
import org.opensearch.core.tasks.TaskId
import org.opensearch.replication.action.bulk.BulkReplicationRequest
import org.opensearch.replication.action.bulk.TransportBulkReplicationAction
import org.opensearch.replication.action.bulk.UpdateBulkTaskStateAction
import org.opensearch.replication.action.bulk.UpdateBulkTaskStateRequest
import org.opensearch.replication.task.ReplicationState
import org.opensearch.replication.task.index.IndexReplicationState
import org.opensearch.replication.task.index.FailedState
import org.opensearch.replication.action.status.ReplicationStatusAction
import org.opensearch.replication.action.status.ShardInfoRequest
import org.opensearch.persistent.PersistentTasksCustomMetadata
import org.opensearch.replication.metadata.state.BulkTaskState
import org.opensearch.replication.util.suspendExecute
import org.opensearch.tasks.CancellableTask
import org.opensearch.tasks.Task
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

enum class BulkOperationType(val label: String) {
    START("bulk_start_replication"),
    STOP("bulk_stop_replication"),
    PAUSE("bulk_pause_replication"),
    RESUME("bulk_resume_replication")
}

class BulkReplicationTask(
    id: Long, type: String, action: String, description: String,
    parentTaskId: TaskId, headers: Map<String, String>,
    private val request: BulkReplicationRequest,
    private val operationType: BulkOperationType,
    private val client: Client,
    private val clusterService: ClusterService,
    private val threadPool: ThreadPool,
    private val bulkBatchSize: Int,
    private val bulkPollTimeoutMinutes: Int,
    private val preResolvedIndices: List<String>,
    private val preFailedIndices: List<FailedIndex>,
    private val transportAction: TransportBulkReplicationAction
) : CancellableTask(id, type, action, description, parentTaskId, headers) {

    companion object {
        private val log = LogManager.getLogger(BulkReplicationTask::class.java)
        private const val POLL_INTERVAL_MS = 3000L
    }

    private val startTime = System.currentTimeMillis()
    private val successCount = AtomicInteger(0)
    private val failedIndices = CopyOnWriteArrayList<FailedIndex>(preFailedIndices)
    private var bulkTaskId: String = ""
    private val pendingCount = AtomicInteger(preResolvedIndices.size)
    private val cancelledCount = AtomicInteger(0)

    private fun isSuccessState(state: ReplicationState) =
        state == ReplicationState.RESTORING || state == ReplicationState.FOLLOWING || state == ReplicationState.MONITORING

    private fun getIndexTaskState(index: String): IndexReplicationState? {
        val persistentTasks = clusterService.state().metadata
            .custom<PersistentTasksCustomMetadata>(PersistentTasksCustomMetadata.TYPE)
        return persistentTasks?.getTask("replication:index:$index")?.state as? IndexReplicationState
    }

    override fun shouldCancelChildrenOnCancellation() = false

    override fun getStatus(): Task.Status = BulkReplicationTaskStatus(
        operationType = operationType.label, pattern = request.pattern,
        startTime = startTime, numSuccess = successCount.get(),
        numFailed = failedIndices.size, numPending = pendingCount.get(),
        numCancelled = cancelledCount.get(), failedIndices = failedIndices.toList()
    )

    fun run(listener: ActionListener<BulkReplicationTaskStatus>, taskId: String) {
        bulkTaskId = taskId
        CoroutineScope(threadPool.coroutineContext(ThreadPool.Names.GENERIC)).launch {
            try {
                val indices = preResolvedIndices
                updateClusterState()
                log.info("BulkReplicationTask.run: Processing ${indices.size} indices for ${operationType.label} pattern=${request.pattern} isCancelled=$isCancelled")

                if (!isCancelled) {
                    if (operationType == BulkOperationType.START || operationType == BulkOperationType.RESUME) {
                        processStartResume(indices)
                    } else {
                        processStopPause(indices)
                    }
                } else {
                    log.info("BulkReplicationTask.run: SKIPPED processing because isCancelled=true")
                }

                if (isCancelled) {
                    val newFailures = failedIndices.size - preFailedIndices.size
                    cancelledCount.set(maxOf(0, indices.size - successCount.get() - newFailures))
                }
                pendingCount.set(0)
                updateClusterState()
                val finalStatus = getStatus() as BulkReplicationTaskStatus
                log.info("BulkReplicationTask.run: COMPLETED success=${finalStatus.numSuccess} failed=${finalStatus.numFailed} pending=${finalStatus.numPending} cancelled=${finalStatus.numCancelled}")
                listener.onResponse(finalStatus)
            } catch (e: kotlinx.coroutines.CancellationException) {
                log.info("BulkReplicationTask.run: CancellationException: ${e.message}")
                pendingCount.set(0)
                cancelledCount.set(maxOf(0, preResolvedIndices.size - successCount.get() - (failedIndices.size - preFailedIndices.size)))
                listener.onResponse(getStatus() as BulkReplicationTaskStatus)
            } catch (e: Exception) {
                log.error("BulkReplicationTask.run: EXCEPTION ${e.javaClass.simpleName}: ${e.message}", e)
                pendingCount.set(0)
                listener.onFailure(e)
            } finally {
                pendingCount.set(0)
                for (attempt in 1..3) {
                    try { updateClusterState(force = true); break } catch (_: Exception) {
                        if (attempt < 3) kotlinx.coroutines.delay(1000)
                    }
                }
            }
        }
    }

    private suspend fun processStartResume(indices: List<String>) {
        val allStarted = CopyOnWriteArrayList<String>()
        val batchFailures = CopyOnWriteArrayList<FailedIndex>()
        val dispatchDone = AtomicBoolean(false)
        val deadline = System.currentTimeMillis() + bulkPollTimeoutMinutes * 60 * 1000L

        val poller = CoroutineScope(threadPool.coroutineContext(ThreadPool.Names.GENERIC)).launch {
            while (!isCancelled && System.currentTimeMillis() < deadline) {
                kotlinx.coroutines.delay(POLL_INTERVAL_MS)
                val snapshot = allStarted.toList()
                if (snapshot.isEmpty()) { if (dispatchDone.get()) break else continue }
                pollIndices(snapshot, allStarted, batchFailures)
                updateClusterState()
            }
        }

        // Dispatch batches with per-batch wait for INIT→RESTORING
        for (batch in indices.chunked(bulkBatchSize)) {
            if (isCancelled) break
            val (started, failures) = dispatchBatch(batch)
            allStarted.addAll(started)
            batchFailures.addAll(failures)
            pendingCount.addAndGet(-failures.size)
            updateClusterState()
            // Brief pause between batches to avoid overwhelming the cluster manager.
            // The bulk restore (done in executeBatchStart) means tasks skip INIT→RESTORING entirely
            // and go straight to InitFollowState, so there's no need to wait for restores.
            if (started.isNotEmpty() && indices.size > bulkBatchSize) {
                kotlinx.coroutines.delay(2000)
            }
        }

        // End-retry: retry failed indices 3 times, 5s apart
        if (batchFailures.isNotEmpty() && !isCancelled) {
            var toRetry = batchFailures.map { it.index }
            for (attempt in 1..3) {
                if (toRetry.isEmpty() || isCancelled) break
                kotlinx.coroutines.delay(5000)
                // Check if any "failed" indices are actually already replicating
                val alreadyRunning = mutableListOf<String>()
                val genuinelyFailed = mutableListOf<String>()
                for (index in toRetry) {
                    val state = getIndexTaskState(index)
                    if (state != null && isSuccessState(state.state)) {
                        alreadyRunning.add(index)
                    } else {
                        genuinelyFailed.add(index)
                    }
                }
                if (alreadyRunning.isNotEmpty()) allStarted.addAll(alreadyRunning)
                if (genuinelyFailed.isNotEmpty()) {
                    val (started, failures) = dispatchBatch(genuinelyFailed)
                    allStarted.addAll(started)
                    toRetry = failures.map { it.index }
                } else {
                    toRetry = emptyList()
                }
                updateClusterState()
            }
            // Permanently fail remaining
            for (index in toRetry) {
                val reason = batchFailures.firstOrNull { it.index == index }?.reason ?: "Failed after 3 retries"
                failedIndices.add(FailedIndex(index, reason))
                pendingCount.decrementAndGet()
            }
        }

        dispatchDone.set(true)
        poller.join()

        // Post-poller retry: check indices still in allStarted
        var retryRemaining = allStarted.toList()
        for (retry in 1..3) {
            if (retryRemaining.isEmpty() || isCancelled) break
            kotlinx.coroutines.delay(5000)
            val stillPending = mutableListOf<String>()
            for (index in retryRemaining) {
                val state = getIndexTaskState(index)
                when {
                    state != null && isSuccessState(state.state) -> {
                        allStarted.remove(index)
                        successCount.incrementAndGet()
                        pendingCount.decrementAndGet()
                    }
                    state != null && state.state == ReplicationState.FAILED -> {
                        allStarted.remove(index)
                        failedIndices.add(FailedIndex(index, getFailureReason(state, index)))
                        pendingCount.decrementAndGet()
                    }
                    else -> stillPending.add(index)
                }
            }
            retryRemaining = stillPending
            updateClusterState()
        }

        // Timeout remaining
        for (index in allStarted) {
            failedIndices.add(FailedIndex(index, "Timed out waiting for replication to confirm started. Replication may still be in progress — check index status."))
            pendingCount.decrementAndGet()
        }
    }

    private suspend fun dispatchBatch(batch: List<String>): Pair<List<String>, List<FailedIndex>> {
        return when (operationType) {
            BulkOperationType.START -> transportAction.executeBatchStart(batch, request)
            BulkOperationType.RESUME -> transportAction.executeBatchResume(batch)
            else -> throw IllegalStateException()
        }
    }

    private suspend fun processStopPause(indices: List<String>) {
        log.info("processStopPause: starting ${operationType.label} for ${indices.size} indices, batchSize=$bulkBatchSize")
        for (batch in indices.chunked(bulkBatchSize)) {
            if (isCancelled) { log.info("processStopPause: cancelled, breaking"); break }
            val (succeeded, failed) = when (operationType) {
                BulkOperationType.STOP -> transportAction.executeBatchStop(batch)
                BulkOperationType.PAUSE -> transportAction.executeBatchPause(batch)
                else -> throw IllegalStateException()
            }
            log.info("processStopPause: batch result: succeeded=${succeeded.size} failed=${failed.size}")
            successCount.addAndGet(succeeded.size)
            failedIndices.addAll(failed)
            pendingCount.addAndGet(-batch.size)
            updateClusterState()
        }
        log.info("processStopPause: DONE totalSuccess=${successCount.get()} totalFailed=${failedIndices.size} pending=${pendingCount.get()}")
    }

    private suspend fun pollIndices(snapshot: List<String>, allStarted: CopyOnWriteArrayList<String>, batchFailures: CopyOnWriteArrayList<FailedIndex>) {
        for (index in snapshot) {
            if (isCancelled) break
            val state = getIndexTaskState(index) ?: continue
            when {
                isSuccessState(state.state) -> {
                    allStarted.remove(index)
                    successCount.incrementAndGet()
                    pendingCount.decrementAndGet()
                }
                state.state == ReplicationState.FAILED -> {
                    allStarted.remove(index)
                    batchFailures.add(FailedIndex(index, getFailureReason(state, index)))
                    pendingCount.decrementAndGet()
                }
            }
        }
    }

    private suspend fun getFailureReason(state: IndexReplicationState, index: String): String {
        return (state as? FailedState)?.errorMsg?.takeIf { it.isNotBlank() }
            ?: try {
                client.suspendExecute(ReplicationStatusAction.INSTANCE,
                    ShardInfoRequest(index, false), injectSecurityContext = true).reason
            } catch (_: Exception) { "Replication task failed" }
    }

    private val lastClusterStateUpdate = java.util.concurrent.atomic.AtomicLong(0)
    private val UPDATE_CLUSTER_STATE_INTERVAL_MS = 5000L

    private suspend fun updateClusterState(force: Boolean = false) {
        val now = System.currentTimeMillis()
        if (!force && now - lastClusterStateUpdate.get() < UPDATE_CLUSTER_STATE_INTERVAL_MS) return
        lastClusterStateUpdate.set(now)
        try {
            client.suspendExecute(UpdateBulkTaskStateAction.INSTANCE, UpdateBulkTaskStateRequest(
                BulkTaskState(bulkTaskId, operationType.label, request.pattern, startTime,
                    successCount.get(), failedIndices.size, pendingCount.get(),
                    cancelledCount.get(), failedIndices.toList())
            ))
        } catch (e: Exception) {
            log.warn("Failed to update cluster state: ${e.message}")
        }
    }
}
