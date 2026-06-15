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
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.sync.Semaphore
import org.opensearch.replication.util.coroutineContext
import org.apache.logging.log4j.LogManager
import org.opensearch.cluster.service.ClusterService
import org.opensearch.core.action.ActionListener
import org.opensearch.core.tasks.TaskId
import org.opensearch.replication.action.bulk.BulkReplicationRequest
import org.opensearch.replication.action.bulk.TransportBulkReplicationAction
import org.opensearch.replication.action.bulk.UpdateBulkTaskStateAction
import org.opensearch.replication.action.bulk.UpdateBulkTaskStateRequest
import org.opensearch.replication.action.index.ReplicateIndexAction
import org.opensearch.replication.action.index.ReplicateIndexRequest
import org.opensearch.replication.action.status.ReplicationStatusAction
import org.opensearch.replication.action.status.ShardInfoRequest
import org.opensearch.replication.action.status.ShardInfoResponse
import org.opensearch.replication.metadata.state.BulkTaskState
import org.opensearch.replication.util.suspendExecute
import org.opensearch.commons.replication.action.ReplicationActions
import org.opensearch.commons.replication.action.StopIndexReplicationRequest
import org.opensearch.tasks.CancellableTask
import org.opensearch.tasks.Task
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

enum class BulkOperationType(val label: String) {
    START("bulk_start_replication"),
    STOP("bulk_stop_replication"),
    PAUSE("bulk_pause_replication"),
    RESUME("bulk_resume_replication")
}

/**
 * Background task that orchestrates bulk replication operations. Spawned by TransportBulkReplicationAction
 * after index resolution; the API returns the task ID immediately so clients can poll for progress.
 *
 * Chunks resolved indices into batches (size from cluster setting) and dispatches each batch to the
 * appropriate executeBatchX method. For START/RESUME, a parallel poller confirms each index reaches
 * SYNCING state (with retries on failure). For STOP/PAUSE, results are counted immediately.
 *
 * Progress is written to cluster state after each batch so the status API returns live numbers.
 * Supports cancellation between batches and timeout for indices that never reach SYNCING.
 */
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
    private val bulkPollTimeoutMinutes: Int,
    private val preResolvedIndices: List<String>,
    private val preFailedIndices: List<FailedIndex>,
    private val transportAction: TransportBulkReplicationAction
) : CancellableTask(id, type, action, description, parentTaskId, headers) {

    companion object {
        private val log = LogManager.getLogger(BulkReplicationTask::class.java)
        private const val POLL_INTERVAL_MS = 3000L
        private const val MAX_POLL_RETRIES = 3
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

    fun run(listener: ActionListener<BulkReplicationTaskStatus>, taskId: String) {
        bulkTaskId = taskId
        CoroutineScope(threadPool.coroutineContext(ThreadPool.Names.GENERIC)).launch {
            try {
                val indices = preResolvedIndices
                updateClusterState()
                log.info("Processing ${indices.size} indices for ${operationType.label} for pattern=${request.pattern}")

                if (!isCancelled) {
                    val allStarted = CopyOnWriteArrayList<String>()
                    val retryCounts = ConcurrentHashMap<String, Int>()
                    val dispatchDone = AtomicBoolean(false)
                    val deadline = System.currentTimeMillis() + bulkPollTimeoutMinutes * 60 * 1000L
                    val semaphore = Semaphore(50)

                    val poller = launch {
                        while (!isCancelled && System.currentTimeMillis() < deadline) {
                            kotlinx.coroutines.delay(POLL_INTERVAL_MS)
                            val snapshot = allStarted.toList()
                            if (snapshot.isEmpty()) { if (dispatchDone.get()) break else continue }
                            supervisorScope {
                                snapshot.map { index -> async {
                                    if (isCancelled) return@async
                                    semaphore.acquire()
                                    try {
                                        val res = client.suspendExecute(ReplicationStatusAction.INSTANCE,
                                            ShardInfoRequest(index, false), injectSecurityContext = true)
                                        when {
                                            res.status == ShardInfoResponse.SYNCING -> {
                                                allStarted.remove(index)
                                                successCount.incrementAndGet()
                                                pendingCount.decrementAndGet()
                                            }
                                            res.status == "FAILED" -> handleFailed(index, retryCounts, allStarted)
                                        }
                                    } catch (_: Exception) {}
                                    finally { semaphore.release() }
                                } }.awaitAll()
                            }
                            updateClusterState()
                        }
                    }

                    // Dispatch batches
                    for (batch in indices.chunked(bulkBatchSize)) {
                        if (isCancelled) break
                        allStarted.addAll(processBatch(batch))
                        updateClusterState()
                    }
                    dispatchDone.set(true)
                    poller.join()

                    // Timed-out indices
                    for (index in allStarted) {
                        failedIndices.add(FailedIndex(index, "Timed out waiting for SYNCING state"))
                        pendingCount.decrementAndGet()
                    }
                }

                if (isCancelled) {
                    val newFailures = failedIndices.size - preFailedIndices.size
                    cancelledCount.set(maxOf(0, indices.size - successCount.get() - newFailures))
                }

                pendingCount.set(0)
                listener.onResponse(getStatus() as BulkReplicationTaskStatus)
                updateClusterState()
            } catch (e: kotlinx.coroutines.CancellationException) {
                log.info("Coroutine cancelled for ${operationType.label}")
                pendingCount.set(0)
                val newFailures = failedIndices.size - preFailedIndices.size
                cancelledCount.set(maxOf(0, preResolvedIndices.size - successCount.get() - newFailures))
                listener.onResponse(getStatus() as BulkReplicationTaskStatus)
            } catch (e: Exception) {
                log.error("Task failed for ${operationType.label}: ${e.message}", e)
                pendingCount.set(0)
                listener.onFailure(e)
            } finally {
                pendingCount.set(0)
                for (attempt in 1..3) {
                    try { updateClusterState(); break } catch (_: Exception) {
                        if (attempt < 3) kotlinx.coroutines.delay(1000)
                    }
                }
            }
        }
    }

    private suspend fun handleFailed(index: String, retryCounts: ConcurrentHashMap<String, Int>, allStarted: CopyOnWriteArrayList<String>) {
        val retries = retryCounts.getOrDefault(index, 0)
        if (retries < MAX_POLL_RETRIES) {
            retryCounts[index] = retries + 1
            try {
                client.suspendExecute(ReplicationActions.STOP_REPLICATION_ACTION_TYPE,
                    StopIndexReplicationRequest(index), injectSecurityContext = true)
                kotlinx.coroutines.delay(1000)
                val alias = request.leaderAlias ?: return
                val req = ReplicateIndexRequest(index, alias, index).also {
                    it.isAutoFollowRequest = true
                    it.useRoles = request.useRoles
                }
                client.suspendExecute(ReplicateIndexAction.INSTANCE, req, injectSecurityContext = true)
            } catch (_: Exception) {}
        } else {
            allStarted.remove(index)
            pendingCount.decrementAndGet()
            failedIndices.add(FailedIndex(index, "Replication failed after $MAX_POLL_RETRIES retries"))
        }
    }

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
            log.warn("Failed to update cluster state for ${operationType.label}: ${e.message}", e)
        }
    }

    private suspend fun processBatch(batch: List<String>): List<String> {
        return when (operationType) {
            BulkOperationType.START -> {
                val (started, failures) = transportAction.executeBatchStart(batch, request)
                failedIndices.addAll(failures)
                pendingCount.addAndGet(-failures.size)
                started
            }
            BulkOperationType.RESUME -> {
                val (started, failures) = transportAction.executeBatchResume(batch)
                failedIndices.addAll(failures)
                pendingCount.addAndGet(-failures.size)
                started
            }
            BulkOperationType.PAUSE -> {
                val (succeeded, failed) = transportAction.executeBatchPause(batch)
                successCount.addAndGet(succeeded.size)
                failedIndices.addAll(failed)
                pendingCount.addAndGet(-batch.size)
                emptyList()
            }
            BulkOperationType.STOP -> {
                val (succeeded, failed) = transportAction.executeBatchStop(batch)
                successCount.addAndGet(succeeded.size)
                failedIndices.addAll(failed)
                pendingCount.addAndGet(-batch.size)
                emptyList()
            }
        }
    }
}
