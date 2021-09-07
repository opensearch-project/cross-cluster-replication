/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.elasticsearch.replication.task.shard

import com.amazon.elasticsearch.replication.ReplicationSettings
import com.amazon.elasticsearch.replication.action.changes.GetChangesAction
import com.amazon.elasticsearch.replication.action.changes.GetChangesRequest
import com.amazon.elasticsearch.replication.action.changes.GetChangesResponse
import com.amazon.elasticsearch.replication.metadata.ReplicationMetadataManager
import com.amazon.elasticsearch.replication.metadata.ReplicationOverallState
import com.amazon.elasticsearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import com.amazon.elasticsearch.replication.metadata.state.getReplicationStateParamsForIndex
import com.amazon.elasticsearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import com.amazon.elasticsearch.replication.task.CrossClusterReplicationTask
import com.amazon.elasticsearch.replication.task.ReplicationState
import com.amazon.elasticsearch.replication.util.indicesService
import com.amazon.elasticsearch.replication.util.stackTraceToString
import com.amazon.elasticsearch.replication.util.suspendExecuteWithRetries
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
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.ElasticsearchTimeoutException
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.ClusterChangedEvent
import org.elasticsearch.cluster.ClusterStateListener
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.index.seqno.RetentionLeaseInvalidRetainingSeqNoException
import org.elasticsearch.index.seqno.RetentionLeaseNotFoundException
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.persistent.PersistentTaskState
import org.elasticsearch.persistent.PersistentTasksNodeService
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.tasks.TaskId
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.NodeNotConnectedException
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
    private val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(clusterService.clusterName.value(), remoteClient)
    private var paused = false

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

    private fun toESException(t: Throwable?): ElasticsearchException {
        if (t is ElasticsearchException) {
            return t
        }
        val msg = t?.message ?: t?.javaClass?.name ?: "Unknown failure encountered"
        return ElasticsearchException(msg, t)
    }


    override suspend fun cleanup() {
        /* This is to minimise overhead of calling an additional listener as
        * it continues to be called even after the task is completed.
         */
        clusterService.removeListener(clusterStateListenerForTaskInterruption)
        this.followerClusterStats.stats.remove(followerShardId)
        if (paused) {
            logDebug("Pausing and not removing lease for index $followerIndexName and shard $followerShardId task")
            return
        }
        retentionLeaseHelper.attemptRetentionLeaseRemoval(leaderShardId, followerShardId)
    }

    private fun addListenerToInterruptTask() {
        clusterService.addListener(clusterStateListenerForTaskInterruption)
    }

    inner class ClusterStateListenerForTaskInterruption : ClusterStateListener {
        override fun clusterChanged(event: ClusterChangedEvent) {
            logDebug("Cluster metadata listener invoked on shard task...")
            if (event.metadataChanged()) {
                val replicationStateParams = getReplicationStateParamsForIndex(clusterService, followerShardId.indexName)
                if (replicationStateParams == null) {
                    if (PersistentTasksNodeService.Status(State.STARTED) == status)
                        cancelTask("Shard replication task received an interrupt.")
                } else if (replicationStateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE] == ReplicationOverallState.PAUSED.name){
                    logInfo("Pause state received for index $followerIndexName. Cancelling $followerShardId task")
                    paused = true
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
        // Adding retention lease at local checkpoint of a node. This makes sure
        // new tasks spawned after node changes/shard movements are handled properly
        logInfo("Adding retentionlease at follower sequence number: ${indexShard.lastSyncedGlobalCheckpoint}")
        retentionLeaseHelper.addRetentionLease(leaderShardId, indexShard.lastSyncedGlobalCheckpoint , followerShardId)
        addListenerToInterruptTask()
        this.followerClusterStats.stats[followerShardId] = FollowerShardMetric()

        // Since this setting is not dynamic, setting update would only reflect after pause-resume or on a new replication job.
        val rateLimiter = Semaphore(replicationSettings.readersPerShard)
        val sequencer = TranslogSequencer(scope, replicationMetadata, followerShardId, leaderAlias, leaderShardId.indexName,
                                          TaskId(clusterService.nodeName, id), client, indexShard.localCheckpoint, followerClusterStats)

        val changeTracker = ShardReplicationChangesTracker(indexShard, replicationSettings)

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
                    } catch (e: ElasticsearchTimeoutException) {
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
                        if (e is ElasticsearchException &&
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

                //renew retention lease with global checkpoint so that any shard that picks up shard replication task has data until then.
                try {
                    retentionLeaseHelper.renewRetentionLease(leaderShardId, indexShard.lastSyncedGlobalCheckpoint, followerShardId)
                    followerClusterStats.stats[followerShardId]!!.followerCheckpoint = indexShard.lastSyncedGlobalCheckpoint

                } catch (ex: Exception) {
                    when (ex) {
                        is RetentionLeaseInvalidRetainingSeqNoException, is RetentionLeaseNotFoundException -> {
                            throw ex
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

    override fun toString(): String {
        return "ShardReplicationTask(from=${leaderAlias}$leaderShardId to=$followerShardId)"
    }

    override fun replicationTaskResponse(): CrossClusterReplicationTaskResponse {
        // Cancellation and valid executions are marked as completed
        return CrossClusterReplicationTaskResponse(ReplicationState.COMPLETED.name)
    }
}
