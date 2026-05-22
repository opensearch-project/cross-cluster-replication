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

package org.opensearch.replication.task.shard

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withContext
import org.opensearch.OpenSearchException
import org.opensearch.OpenSearchTimeoutException
import org.opensearch.action.admin.indices.delete.DeleteIndexAction
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.logging.Loggers
import org.opensearch.core.index.shard.ShardId
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.tasks.TaskId
import org.opensearch.index.seqno.RetentionLeaseInvalidRetainingSeqNoException
import org.opensearch.index.seqno.RetentionLeaseNotFoundException
import org.opensearch.replication.ReplicationSettings
import org.opensearch.replication.action.changes.GetChangesAction
import org.opensearch.replication.action.changes.GetChangesRequest
import org.opensearch.replication.action.changes.GetChangesResponse
import org.opensearch.commons.replication.action.ReplicationActions.INTERNAL_STOP_REPLICATION_ACTION_TYPE
import org.opensearch.commons.replication.action.StopIndexReplicationRequest
import org.opensearch.index.IndexNotFoundException
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.metadata.ReplicationOverallState
import org.opensearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import org.opensearch.replication.util.indicesService
import org.opensearch.replication.util.stackTraceToString
import org.opensearch.replication.util.suspendExecuteWithRetries
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.NodeNotConnectedException
import org.opensearch.transport.client.Client
import java.util.concurrent.atomic.AtomicBoolean

/**
 * In-memory replacement for [ShardReplicationTask]. Owns the runtime state needed to
 * replicate operations for a single follower primary shard from its corresponding leader
 * shard.
 *
 * Lifecycle is managed by [NodeReplicationController]:
 *   - [start] launches the replicate loop on this context's coroutine scope.
 *   - [stop] cancels the scope; in-flight work is cancelled cooperatively.
 *   - [renewLeaseIfStale] is invoked by the controller's idle-lease timer.
 *
 * On terminal failure the context updates [org.opensearch.replication.metadata.store.ReplicationMetadata]
 * to PAUSED, which cascades to other nodes' controllers and to the index task. There is no
 * separate failure-state held here beyond the running/stopped distinction.
 */
@ObsoleteCoroutinesApi
class ShardReplicationContext(
    val leaderAlias: String,
    val leaderShardId: ShardId,
    val followerShardId: ShardId,
    private val clusterService: ClusterService,
    private val threadPool: ThreadPool,
    private val client: Client,
    private val replicationMetadataManager: ReplicationMetadataManager,
    private val replicationSettings: ReplicationSettings,
    private val followerClusterStats: FollowerClusterStats,
) {
    private val log = Loggers.getLogger(javaClass, followerShardId)!!

    private val remoteClient = client.getRemoteClusterClient(leaderAlias)
    private val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(
        clusterService.clusterName.value(),
        clusterService.state().metadata.clusterUUID(),
        remoteClient
    )

    @Volatile
    var lastLeaseRenewalMillis: Long = System.currentTimeMillis()
        private set

    private val initialBackoffMillis = 1000L
    private var backOffForRetry = initialBackoffMillis
    private val maxTimeOut = 60000L
    private val factor = 2.0

    private val started = AtomicBoolean(false)
    private val stopped = AtomicBoolean(false)
    private var supervisor: Job? = null

    val followerIndexName: String get() = followerShardId.indexName

    /**
     * Starts the replicate loop. Idempotent: if already started, no-op.
     * Caller is expected to hold a reference to this context for its lifetime.
     */
    fun start(parentScope: CoroutineScope) {
        if (!started.compareAndSet(false, true)) {
            log.debug("ShardReplicationContext already started for $followerShardId")
            return
        }
        supervisor = parentScope.launch {
            try {
                runReplicateLoop()
            } catch (e: CancellationException) {
                log.info("ShardReplicationContext for $followerShardId cancelled")
            } catch (e: Throwable) {
                log.error("ShardReplicationContext for $followerShardId terminated with error: ${e.stackTraceToString()}")
                pauseIndexOnFatalFailure(e)
            }
        }
    }

    /**
     * Cancels the supervisor job. Cooperative — in-flight work will exit at the next
     * cancellation check. Idempotent.
     */
    fun stop() {
        if (!stopped.compareAndSet(false, true)) return
        supervisor?.cancel(CancellationException("ShardReplicationContext stop requested for $followerShardId"))
        followerClusterStats.stats.remove(followerShardId)
    }

    /**
     * Invoked by [NodeReplicationController]'s idle-lease timer. Renews the lease only if
     * the last renewal age exceeds the threshold — for active shards this is a no-op
     * because the replicate loop renews on every successful batch.
     */
    suspend fun renewLeaseIfStale(thresholdMillis: Long) {
        if (System.currentTimeMillis() - lastLeaseRenewalMillis <= thresholdMillis) return
        try {
            val indexShard = indicesService.indexServiceSafe(followerShardId.index).getShard(followerShardId.id)
            retentionLeaseHelper.renewRetentionLease(
                leaderShardId,
                indexShard.lastSyncedGlobalCheckpoint + 1,
                followerShardId
            )
            lastLeaseRenewalMillis = System.currentTimeMillis()
            log.info("Idle-shard lease renewed for $followerShardId at seqNo ${indexShard.lastSyncedGlobalCheckpoint + 1}")
        } catch (e: Exception) {
            log.warn("Idle-shard lease renewal failed for $followerShardId: ${e.message}")
        }
    }

    @ObsoleteCoroutinesApi
    private suspend fun runReplicateLoop() {
        // Resolve replication metadata once at start. The replay path requires it for retry plumbing.
        val replicationMetadata = replicationMetadataManager.getIndexReplicationMetadata(
            followerIndexName,
            fetch_from_primary = true
        )

        val followerIndexService = indicesService.indexServiceSafe(followerShardId.index)
        val indexShard = followerIndexService.getShard(followerShardId.id)
        log.info("Shard replication started: leaderShard=$leaderShardId, followerShard=$followerShardId, " +
                "localCheckpoint=${indexShard.localCheckpoint}, globalCheckpoint=${indexShard.lastSyncedGlobalCheckpoint}")

        try {
            retentionLeaseHelper.renewRetentionLease(
                leaderShardId,
                indexShard.lastSyncedGlobalCheckpoint + 1,
                followerShardId
            )
            lastLeaseRenewalMillis = System.currentTimeMillis()
            log.debug("Initial retention lease renewed for $followerShardId at seqNo ${indexShard.lastSyncedGlobalCheckpoint + 1}")
        } catch (e: Exception) {
            log.error("Initial retention lease renewal failed for $followerShardId: ${e.stackTraceToString()}")
        }

        followerClusterStats.stats[followerShardId] = FollowerShardMetric()
        followerClusterStats.stats[followerShardId]!!.followerCheckpoint = indexShard.localCheckpoint
        // In case the shard task starts on a new node and there are no active writes on the leader shard, leader
        // checkpoint never gets initialized and defaults to 0. Initialize it to follower's localCheckpoint as the
        // leader shard is guaranteed to be at least at this position.
        followerClusterStats.stats[followerShardId]!!.leaderCheckpoint = indexShard.localCheckpoint

        // Wrap the actual replicate body in supervisorScope so that one failed reader does not bring down the
        // outer coroutine. Failures inside readers are caught and recorded; persistent failures terminate the
        // outer scope and trigger pauseIndexOnFatalFailure via the catch in start().
        var fatal: Throwable? = null
        val handler = CoroutineExceptionHandler { _, exception ->
            log.error("ShardReplicationContext caught downstream exception: ${exception.stackTraceToString()}")
            fatal = exception
        }

        supervisorScope {
            launch(handler) {
                replicateInner(replicationMetadata, indexShard)
            }
        }

        if (fatal != null) {
            withContext(NonCancellable) {
                throw fatal!!
            }
        }
    }

    @ObsoleteCoroutinesApi
    private suspend fun replicateInner(
        replicationMetadata: org.opensearch.replication.metadata.store.ReplicationMetadata,
        indexShard: org.opensearch.index.shard.IndexShard,
    ) {
        // Since this setting is not dynamic, setting update would only reflect after pause-resume or on a new
        // replication job.
        val rateLimiter = Semaphore(replicationSettings.readersPerShard)
        val sequencer = TranslogSequencer(
            this.coroutineScopeForSequencer(),
            replicationMetadata,
            followerShardId,
            leaderAlias,
            leaderShardId.indexName,
            TaskId(clusterService.nodeName, 0L), // No persistent parent task; placeholder TaskId for legacy plumbing
            client,
            indexShard.localCheckpoint,
            followerClusterStats,
            replicationSettings.writersPerShard
        )

        val changeTracker = ShardReplicationChangesTracker(indexShard, replicationSettings)

        coroutineScope {
            while (isActive) {
                rateLimiter.acquire()
                launch {
                    val batchToFetch = changeTracker.requestBatchToFetch()
                    val fromSeqNo = batchToFetch.first
                    val toSeqNo = batchToFetch.second
                    try {
                        val changesResponse = getChanges(replicationMetadata, fromSeqNo, toSeqNo)
                        log.info("Got ${changesResponse.changes.size} changes starting from seqNo: $fromSeqNo")
                        sequencer.send(changesResponse)
                        changeTracker.updateBatchFetched(
                            true, fromSeqNo, toSeqNo,
                            changesResponse.changes.lastOrNull()?.seqNo() ?: fromSeqNo - 1,
                            changesResponse.lastSyncedGlobalCheckpoint
                        )
                        backOffForRetry = initialBackoffMillis
                    } catch (e: CancellationException) {
                        // Cooperative cancellation — propagate without pausing the index.
                        throw e
                    } catch (e: OpenSearchTimeoutException) {
                        // Long-poll timeout — leader had no new ops in 1 minute. Re-poll.
                        log.info("Timed out waiting for new changes. Current seqNo: $fromSeqNo. $e")
                        changeTracker.updateBatchFetched(false, fromSeqNo, toSeqNo, fromSeqNo - 1, -1)
                    } catch (e: NodeNotConnectedException) {
                        // Stats entry may already be gone if stop() raced ahead — drop the metric in that case.
                        followerClusterStats.stats[followerShardId]?.opsReadFailures?.addAndGet(1)
                        log.info("Node not connected to $leaderAlias, retrying with different node. seqNo=$fromSeqNo: ${e.message}")
                        delay(backOffForRetry)
                        backOffForRetry = (backOffForRetry * factor).toLong().coerceAtMost(maxTimeOut)
                        changeTracker.updateBatchFetched(false, fromSeqNo, toSeqNo, fromSeqNo - 1, -1)
                    } catch (e: Exception) {
                        followerClusterStats.stats[followerShardId]?.opsReadFailures?.addAndGet(1)
                        log.info("Unable to get changes from seqNo: $fromSeqNo. ${e.stackTraceToString()}")

                        if (e is IllegalArgumentException &&
                            e.message?.contains("ReleasableBytesStreamOutput cannot hold more than") == true) {
                            log.error("Hit 2GB limit with current batch size ${changeTracker.batchSizeSettings().getEffectiveBatchSize()}. Reducing batch size.")
                            changeTracker.reduceBatchSize()
                            log.error("Reduced batch size to ${changeTracker.batchSizeSettings().getEffectiveBatchSize()}. Retrying immediately.")
                            changeTracker.updateBatchFetched(false, fromSeqNo, toSeqNo, fromSeqNo - 1, -1)
                            return@launch
                        }

                        changeTracker.updateBatchFetched(false, fromSeqNo, toSeqNo, fromSeqNo - 1, -1)
                        val range4xx = 400.rangeTo(499)
                        if (e is OpenSearchException && range4xx.contains(e.status().status)) {
                            if (e.status().status == RestStatus.TOO_MANY_REQUESTS.status) {
                                followerClusterStats.stats[followerShardId]?.opsReadThrottles?.addAndGet(1)
                            } else {
                                throw e
                            }
                        }
                        delay(backOffForRetry)
                        backOffForRetry = (backOffForRetry * factor).toLong().coerceAtMost(maxTimeOut)
                    } finally {
                        rateLimiter.release()
                    }
                }

                // Renew lease after each batch dispatch with the follower's current globalCheckpoint + 1.
                try {
                    retentionLeaseHelper.renewRetentionLease(
                        leaderShardId,
                        indexShard.lastSyncedGlobalCheckpoint + 1,
                        followerShardId
                    )
                    lastLeaseRenewalMillis = System.currentTimeMillis()
                } catch (ex: Exception) {
                    when (ex) {
                        is RetentionLeaseNotFoundException -> {
                            log.error("Retention lease not found for $leaderShardId/$followerShardId — replication cannot continue")
                            throw ex
                        }
                        is RetentionLeaseInvalidRetainingSeqNoException -> {
                            if (System.currentTimeMillis() - lastLeaseRenewalMillis > replicationSettings.leaseRenewalMaxFailureDuration.millis) {
                                log.error("Retention lease renewal has been failing for last ${replicationSettings.leaseRenewalMaxFailureDuration.minutes} minutes")
                                throw ex
                            } else {
                                log.error("Retention lease renewal failed. Ignoring. ${ex.message}")
                            }
                        }
                        else -> log.info("Exception renewing retention lease. Not an issue: ${ex.stackTraceToString()}")
                    }
                }
            }
        }
        sequencer.close()
    }

    private fun coroutineScopeForSequencer(): CoroutineScope {
        // The sequencer needs a CoroutineScope to attach its actor to. We create a child scope tied to the
        // supervisor job so cancellation of the context cleanly cancels the sequencer.
        val parent = supervisor ?: error("ShardReplicationContext.start() must be called before sequencer scope is requested")
        return CoroutineScope(SupervisorJob(parent) + Dispatchers.Default)
    }

    private suspend fun getChanges(
        replicationMetadata: org.opensearch.replication.metadata.store.ReplicationMetadata,
        fromSeqNo: Long,
        toSeqNo: Long,
    ): GetChangesResponse {
        val request = GetChangesRequest(leaderShardId, fromSeqNo, toSeqNo)
        val resp = remoteClient.suspendExecuteWithRetries(
            replicationMetadata = replicationMetadata,
            action = GetChangesAction.INSTANCE,
            req = request,
            log = log
        )
        // Stats entry may have been removed concurrently by stop() — skip the update in that case.
        followerClusterStats.stats[followerShardId]?.let { stat ->
            stat.leaderCheckpoint = resp.lastSyncedGlobalCheckpoint
            stat.opsRead.addAndGet(resp.changes.size.toLong())
        }
        return resp
    }

    private suspend fun pauseIndexOnFatalFailure(cause: Throwable) {
        // Use the fully-qualified class name in the reason so existing helpers/tests that grep for FQNs (e.g.
        // "org.opensearch.indices.IndexClosedException") continue to match.
        val reason = "Shard $followerShardId failed: ${cause.javaClass.name} - ${cause.message ?: "no message"}"

        // If the failure is because the leader index no longer exists AND replicate-index-deletion is enabled,
        // stop replication entirely (which cascades to follower index deletion via IndexReplicationTask). This
        // preserves the cleanup path that used to live in IndexReplicationTask.MONITORING.
        val replicateDelete = clusterService.clusterSettings.get(
            org.opensearch.replication.ReplicationPlugin.REPLICATION_REPLICATE_INDEX_DELETION
        )
        if (replicateDelete && cause is IndexNotFoundException && cause.message?.contains(leaderShardId.indexName) == true) {
            try {
                log.warn("Leader index ${leaderShardId.indexName} unavailable; stopping replication on $followerIndexName")
                client.execute(
                    INTERNAL_STOP_REPLICATION_ACTION_TYPE,
                    StopIndexReplicationRequest(followerIndexName)
                ).actionGet()
                // The legacy IndexReplicationTask cleanup path used `isLeaderIndexDeleted` to drive a follow-on
                // DeleteIndex when replicate-delete was enabled. With per-shard tasks gone, that flag never gets
                // set, so we issue the delete here. Idempotent — a concurrent context on another shard may have
                // already deleted the index, in which case IndexNotFoundException is benign.
                try {
                    val storedContext = client.threadPool().threadContext.stashContext()
                    try {
                        client.execute(DeleteIndexAction.INSTANCE, DeleteIndexRequest(followerIndexName)).actionGet()
                    } finally {
                        storedContext.close()
                    }
                } catch (e: IndexNotFoundException) {
                    log.debug("Follower index $followerIndexName already deleted")
                } catch (e: Exception) {
                    log.warn("Failed to delete follower index $followerIndexName after leader-deletion stop: ${e.message}")
                }
                return
            } catch (e: Exception) {
                log.warn("Stop-on-leader-deletion failed for $followerIndexName, falling through to pause: ${e.message}")
            }
        }

        try {
            log.warn("Pausing replication for index $followerIndexName due to fatal shard failure: $reason")
            replicationMetadataManager.updateIndexReplicationState(
                followerIndexName,
                ReplicationOverallState.PAUSED,
                reason
            )
        } catch (e: Exception) {
            // Update may have already been done by another shard's context. Idempotent — log and move on.
            log.warn("Could not update replication state to PAUSED for $followerIndexName (may already be paused): ${e.message}")
        }
    }

    override fun toString(): String {
        return "ShardReplicationContext(leader=$leaderAlias$leaderShardId, follower=$followerShardId, started=${started.get()}, stopped=${stopped.get()})"
    }
}
