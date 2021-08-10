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
import org.opensearch.client.Client
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.logging.Loggers
import org.opensearch.index.seqno.RetentionLeaseInvalidRetainingSeqNoException
import org.opensearch.index.seqno.RetentionLeaseNotFoundException
import org.opensearch.index.shard.ShardId
import org.opensearch.persistent.PersistentTaskState
import org.opensearch.persistent.PersistentTasksNodeService
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.TaskId
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.NodeNotConnectedException
import java.time.Duration


class ShardReplicationTask(id: Long, type: String, action: String, description: String, parentTask: TaskId,
                           params: ShardReplicationParams, executor: String, clusterService: ClusterService,
                           threadPool: ThreadPool, client: Client, replicationMetadataManager: ReplicationMetadataManager,
                           replicationSettings: ReplicationSettings)
    : CrossClusterReplicationTask(id, type, action, description, parentTask, emptyMap(),
                                  executor, clusterService, threadPool, client, replicationMetadataManager, replicationSettings) {

    override val leaderAlias: String = params.leaderAlias
    override val followerIndexName: String = params.followerShardId.indexName
    private val leaderShardId = params.leaderShardId
    private val followerShardId = params.followerShardId
    private val remoteClient = client.getRemoteClusterClient(leaderAlias)
    private val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(clusterService.clusterName.value(), remoteClient)
    private var paused = false
    private val backOffForNodeDiscovery = 1000L

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

        // Since this setting is not dynamic, setting update would only reflect after pause-resume or on a new replication job.
        val rateLimiter = Semaphore(replicationSettings.readersPerShard)
        val sequencer = TranslogSequencer(scope, replicationMetadata, followerShardId, leaderAlias, leaderShardId.indexName,
                                          TaskId(clusterService.nodeName, id), client, indexShard.localCheckpoint)

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
                    } catch (e: OpenSearchTimeoutException) {
                        logInfo("Timed out waiting for new changes. Current seqNo: $fromSeqNo. $e")
                        changeTracker.updateBatchFetched(false, fromSeqNo, toSeqNo, fromSeqNo - 1,-1)
                    } catch (e: NodeNotConnectedException) {
                        logInfo("Node not connected. Retrying request using a different node. ${e.stackTraceToString()}")
                        delay(backOffForNodeDiscovery)
                        changeTracker.updateBatchFetched(false, fromSeqNo, toSeqNo, fromSeqNo - 1,-1)
                    } catch (e: Exception) {
                        logInfo("Unable to get changes from seqNo: $fromSeqNo. ${e.stackTraceToString()}")
                        changeTracker.updateBatchFetched(false, fromSeqNo, toSeqNo, fromSeqNo - 1,-1)

                        // Propagate 4xx exceptions up the chain and halt replication as they are irrecoverable
                        val range4xx = 400.rangeTo(499)
                        if (e is OpenSearchException &&
                            range4xx.contains(e.status().status) &&
                            e.status().status != RestStatus.TOO_MANY_REQUESTS.status) {
                            throw e
                        }
                    } finally {
                        rateLimiter.release()
                    }
                }

                //renew retention lease with global checkpoint so that any shard that picks up shard replication task has data until then.
                try {
                    retentionLeaseHelper.renewRetentionLease(leaderShardId, indexShard.lastSyncedGlobalCheckpoint, followerShardId)
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
        return remoteClient.suspendExecuteWithRetries(replicationMetadata = replicationMetadata,
                action = GetChangesAction.INSTANCE, req = request, log = log)
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
