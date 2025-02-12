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

import org.opensearch.replication.ReplicationSettings
import org.opensearch.replication.action.changes.GetChangesAction
import org.opensearch.replication.action.changes.GetChangesRequest
import org.opensearch.replication.action.changes.GetChangesResponse
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.metadata.ReplicationOverallState
import org.opensearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import org.opensearch.replication.metadata.state.getReplicationStateParamsForIndex
import org.opensearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import org.opensearch.replication.task.CrossClusterReplicationTask
import org.opensearch.replication.task.ReplicationState
import org.opensearch.replication.util.indicesService
import org.opensearch.replication.util.stackTraceToString
import org.opensearch.replication.util.suspendExecuteWithRetries
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withContext
import org.opensearch.OpenSearchException
import org.opensearch.OpenSearchTimeoutException
import org.opensearch.transport.client.Client
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.logging.Loggers
import org.opensearch.index.seqno.RetentionLeaseActions
import org.opensearch.index.seqno.RetentionLeaseInvalidRetainingSeqNoException
import org.opensearch.index.seqno.RetentionLeaseNotFoundException
import org.opensearch.core.index.shard.ShardId
import org.opensearch.persistent.PersistentTaskState
import org.opensearch.persistent.PersistentTasksNodeService
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.tasks.TaskId
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.NodeNotConnectedException
import java.time.Duration


class ShardReplicationTask(id: Long, type: String, action: String, description: String, parentTask: TaskId,
                           params: ShardReplicationParams, executor: String, clusterService: ClusterService,
                           threadPool: ThreadPool, client: Client, replicationMetadataManager: ReplicationMetadataManager,
                           replicationSettings: ReplicationSettings, private val followerClusterStats: FollowerClusterStats)
    : CrossClusterReplicationTask(id, type, action, description, parentTask, emptyMap(),
                                  executor, clusterService, threadPool, client, replicationMetadataManager, replicationSettings) {

    override val leaderAlias: String = params.leaderAlias
    override val followerIndexName: String = params.followerShardId.indexName
    private val leaderShardId = params.leaderShardId
    private val followerShardId = params.followerShardId
    private val remoteClient = client.getRemoteClusterClient(leaderAlias)
    private val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(clusterService.clusterName.value(), clusterService.state().metadata.clusterUUID(), remoteClient)
    private var lastLeaseRenewalMillis = System.currentTimeMillis()

    //Start backOff for exceptions with a second
    private val initialBackoffMillis = 1000L
    //Start backOff for exceptions with a second
    private var backOffForRetry = initialBackoffMillis
    //Max timeout for backoff
    private val maxTimeOut = 60000L
    //Backoff factor after every retry
    private val factor = 2.0

    private val clusterStateListenerForTaskInterruption = ClusterStateListenerForTaskInterruption()

    override val log = Loggers.getLogger(javaClass, followerShardId)!!

    companion object {
        fun taskIdForShard(shardId: ShardId) = "replication:${shardId}"
    }

    @ObsoleteCoroutinesApi
    override suspend fun execute(scope: CoroutineScope, initialState: PersistentTaskState?) {
        try {
            // The CoroutineExceptionHandler installed is mainly used to catch the exception from replication replay
            // logic and persist the failure reason.
            var downstreamException: Throwable? = null
            val handler = CoroutineExceptionHandler { _, exception ->
                logError("ShardReplicationTask: Caught downstream exception ${exception.stackTraceToString()}")
                downstreamException = exception
            }

            // Wrap the actual replication replay logic in SupervisorCoroutine and an inner coroutine so that we have
            // better control over exception propagation. Essentially any failures from inner replication logic will
            // not cancel the parent coroutine and the exception is caught by the installed CoroutineExceptionHandler
            //
            // The only path for cancellation of this outer coroutine is external explicit cancellation (pause logic,
            // task being cancelled by API etc)
            //
            // Checkout out the following for details
            // https://kotlinlang.org/docs/exception-handling.html#supervision-scope
            supervisorScope {
                launch(handler) {
                    replicate(this)
                }
            }

            // Non-null downstreamException implies, exception in inner replication code. In such cases we mark and
            // capture the FailedState and wait for parent IndexReplicationTask to take action.
            //
            // Note that we don't take the action to pause/stop directly from this ShardReplicationTask since
            // IndexReplicationTask can choose the appropriate action based on failures seen from multiple shards. This
            // approach also avoids contention due to concurrency. Finally it keeps the scope of responsibility of
            // ShardReplicationTask to ShardReplicationTask alone.
            if (null != downstreamException) {
                // Explicit cast is required for changing closures
                val throwable: Throwable = downstreamException as Throwable

                withContext(NonCancellable) {
                    logInfo("Going to mark ShardReplicationTask as Failed with ${throwable.stackTraceToString()}")
                    try {
                        updateTaskState(FailedState(toESException(throwable)))
                    } catch (inner: Exception) {
                        logInfo("Encountered exception while trying to persist failed state ${inner.stackTraceToString()}")
                        // We are not propagating failure here and will let the shard task be failed after waiting.
                    }
                }

                // After marking FailedState, IndexReplicationTask will action on it by pausing or stopping all shard
                // replication tasks. This ShardReplicationTask should also thus receive the pause/stop via
                // cancellation. We thus wait for waitMillis duration.
                val waitMillis = Duration.ofMinutes(10).toMillis()
                logInfo("Waiting $waitMillis millis for IndexReplicationTask to respond to failure of shard task")
                delay(waitMillis)

                // If nothing happened, we propagate exception and mark the task as failed.
                throw throwable
            }

        } catch (e: CancellationException) {
            // Nothing to do here and we don't propagate cancellation exception further
            logInfo("Received cancellation of ShardReplicationTask ${e.stackTraceToString()}")
        }
    }

    private fun toESException(t: Throwable?): OpenSearchException {
        if (t is OpenSearchException) {
            return t
        }
        val msg = t?.message ?: t?.javaClass?.name ?: "Unknown failure encountered"
        return OpenSearchException(msg, t)
    }


    override suspend fun cleanup() {
        /* This is to minimise overhead of calling an additional listener as
        * it continues to be called even after the task is completed.
         */
        clusterService.removeListener(clusterStateListenerForTaskInterruption)
        this.followerClusterStats.stats.remove(followerShardId)
    }

    private fun addListenerToInterruptTask() {
        clusterService.addListener(clusterStateListenerForTaskInterruption)
    }

    inner class ClusterStateListenerForTaskInterruption : ClusterStateListener {
        override fun clusterChanged(event: ClusterChangedEvent) {
            logDebug("Cluster metadata listener invoked on shard task...")
            if (event.metadataChanged()) {
                val replicationStateParams = getReplicationStateParamsForIndex(clusterService, followerShardId.indexName)
                logDebug("Replication State Params are fetched from cluster state")
                if (replicationStateParams == null) {
                    if (PersistentTasksNodeService.Status(State.STARTED) == status)
                        cancelTask("Shard replication task received an interrupt.")
                } else if (replicationStateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE] == ReplicationOverallState.PAUSED.name){
                    logInfo("Pause state received for index $followerIndexName. Cancelling $followerShardId task")
                    cancelTask("Shard replication task received pause.")
                }
            }
        }
    }

    override fun indicesOrShards() = listOf(followerShardId)

    @ObsoleteCoroutinesApi
    private suspend fun replicate(scope: CoroutineScope) {
        updateTaskState(FollowingState)
        val followerIndexService = indicesService.indexServiceSafe(followerShardId.index)
        val indexShard = followerIndexService.getShard(followerShardId.id)

        try {
            //Retention leases preserve the operations including and starting from the retainingSequenceNumber we specify when we take the lease .
            //hence renew retention lease with lastSyncedGlobalCheckpoint + 1
            retentionLeaseHelper.renewRetentionLease(leaderShardId, indexShard.lastSyncedGlobalCheckpoint + 1, followerShardId)
        } catch (ex: Exception) {
            // In case of a failure, we just log it and move on. All failures scenarios are being handled below with
            // retries and backoff depending on exception.
            log.error("Retention lease renewal failed: ${ex.stackTraceToString()}")
        }

        addListenerToInterruptTask()
        this.followerClusterStats.stats[followerShardId] = FollowerShardMetric()

        // Since this setting is not dynamic, setting update would only reflect after pause-resume or on a new replication job.
        val rateLimiter = Semaphore(replicationSettings.readersPerShard)
        val sequencer = TranslogSequencer(scope, replicationMetadata, followerShardId, leaderAlias, leaderShardId.indexName,
                                          TaskId(clusterService.nodeName, id), client, indexShard.localCheckpoint, followerClusterStats, replicationSettings.writersPerShard)

        val changeTracker = ShardReplicationChangesTracker(indexShard, replicationSettings)
        followerClusterStats.stats[followerShardId]!!.followerCheckpoint = indexShard.localCheckpoint
        // In case the shard task starts on a new node and there are no active writes on the leader shard, leader checkpoint
        // never gets initialized and defaults to 0. To get around this, we set the leaderCheckpoint to follower shard's
        // localCheckpoint as the leader shard is guaranteed to equal or more.
        followerClusterStats.stats[followerShardId]!!.leaderCheckpoint = indexShard.localCheckpoint
        coroutineScope {
            while (isActive) {
                rateLimiter.acquire()
                launch {
                    logDebug("Spawning the reader")
                    val batchToFetch = changeTracker.requestBatchToFetch()
                    val fromSeqNo = batchToFetch.first
                    val toSeqNo = batchToFetch.second

                    try {
                        logDebug("Getting changes $fromSeqNo-$toSeqNo")
                        val changesResponse = getChanges(fromSeqNo, toSeqNo)
                        logInfo("Got ${changesResponse.changes.size} changes starting from seqNo: $fromSeqNo")
                        sequencer.send(changesResponse)
                        logDebug("pushed to sequencer $fromSeqNo-$toSeqNo")
                        changeTracker.updateBatchFetched(true, fromSeqNo, toSeqNo, changesResponse.changes.lastOrNull()?.seqNo() ?: fromSeqNo - 1,
                            changesResponse.lastSyncedGlobalCheckpoint)
                        //reset backoff after every successful getChanges call
                        backOffForRetry = initialBackoffMillis
                    } catch (e: OpenSearchTimeoutException) {
                        //TimeoutException is thrown if leader fails to send new changes in 1 minute, so we dont need a backoff again here for this exception
                        logInfo("Timed out waiting for new changes. Current seqNo: $fromSeqNo. $e")
                        changeTracker.updateBatchFetched(false, fromSeqNo, toSeqNo, fromSeqNo - 1,-1)
                    } catch (e: NodeNotConnectedException) {
                        followerClusterStats.stats[followerShardId]!!.opsReadFailures.addAndGet(1)
                        logInfo("Node not connected. Retrying request using a different node. ${e.stackTraceToString()}")
                        delay(backOffForRetry)
                        backOffForRetry = (backOffForRetry * factor).toLong().coerceAtMost(maxTimeOut)
                        changeTracker.updateBatchFetched(false, fromSeqNo, toSeqNo, fromSeqNo - 1,-1)
                    } catch (e: Exception) {
                        followerClusterStats.stats[followerShardId]!!.opsReadFailures.addAndGet(1)
                        logInfo("Unable to get changes from seqNo: $fromSeqNo. ${e.stackTraceToString()}")
                        changeTracker.updateBatchFetched(false, fromSeqNo, toSeqNo, fromSeqNo - 1,-1)
                        // Propagate 4xx exceptions up the chain and halt replication as they are irrecoverable
                        val range4xx = 400.rangeTo(499)
                        if (e is OpenSearchException &&
                                range4xx.contains(e.status().status) ) {
                            if (e.status().status == RestStatus.TOO_MANY_REQUESTS.status) {
                                followerClusterStats.stats[followerShardId]!!.opsReadThrottles.addAndGet(1)
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


                //Retention leases preserve the operations including and starting from the retainingSequenceNumber we specify when we take the lease .
                //hence renew retention lease with lastSyncedGlobalCheckpoint + 1 so that any shard that picks up shard replication task has data until then.
                try {
                    retentionLeaseHelper.renewRetentionLease(leaderShardId, indexShard.lastSyncedGlobalCheckpoint + 1, followerShardId)
                    lastLeaseRenewalMillis = System.currentTimeMillis()
                } catch (ex: Exception) {
                    when (ex) {
                        is RetentionLeaseNotFoundException ->  throw ex
                        is RetentionLeaseInvalidRetainingSeqNoException -> {
                            if (System.currentTimeMillis() - lastLeaseRenewalMillis >  replicationSettings.leaseRenewalMaxFailureDuration.millis) {
                                log.error("Retention lease renewal has been failing for last " +
                                        "${replicationSettings.leaseRenewalMaxFailureDuration.minutes} minutes")
                                throw ex
                            } else {
                                log.error("Retention lease renewal failed. Ignoring. ${ex.message}")
                            }
                        }
                        else -> logInfo("Exception renewing retention lease. Not an issue - ${ex.stackTraceToString()}")
                    }
                }
            }
        }
        sequencer.close()
    }

    private suspend fun getChanges(fromSeqNo: Long, toSeqNo: Long): GetChangesResponse {
        val remoteClient = client.getRemoteClusterClient(leaderAlias)
        val request = GetChangesRequest(leaderShardId, fromSeqNo, toSeqNo)

        var changesResp =  remoteClient.suspendExecuteWithRetries(replicationMetadata = replicationMetadata,
                action = GetChangesAction.INSTANCE, req = request, log = log)
        followerClusterStats.stats[followerShardId]!!.leaderCheckpoint = changesResp.lastSyncedGlobalCheckpoint
        followerClusterStats.stats[followerShardId]!!.opsRead.addAndGet(changesResp.changes.size.toLong())
        return changesResp
    }
    private fun logDebug(msg: String) {
        log.debug("${Thread.currentThread().name}: $msg")
    }
    private fun logInfo(msg: String) {
        log.info("${Thread.currentThread().name}: $msg")
    }
    private fun logError(msg: String) {
        log.error("${Thread.currentThread().name}: $msg")
    }

    override suspend fun setReplicationMetadata() {
        this.replicationMetadata = replicationMetadataManager.getIndexReplicationMetadata(followerIndexName, fetch_from_primary = true)
    }

    override fun toString(): String {
        return "ShardReplicationTask(from=${leaderAlias}$leaderShardId to=$followerShardId)"
    }

    override fun replicationTaskResponse(): CrossClusterReplicationTaskResponse {
        // Cancellation and valid executions are marked as completed
        return CrossClusterReplicationTaskResponse(ReplicationState.COMPLETED.name)
    }
}
