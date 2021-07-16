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

import com.amazon.elasticsearch.replication.ReplicationException
import com.amazon.elasticsearch.replication.ReplicationPlugin.Companion.REPLICATION_CHANGE_BATCH_SIZE
import com.amazon.elasticsearch.replication.ReplicationPlugin.Companion.REPLICATION_PARALLEL_READ_PER_SHARD
import com.amazon.elasticsearch.replication.ReplicationSettings
import com.amazon.elasticsearch.replication.action.changes.GetChangesAction
import com.amazon.elasticsearch.replication.action.changes.GetChangesRequest
import com.amazon.elasticsearch.replication.action.changes.GetChangesResponse
import com.amazon.elasticsearch.replication.action.pause.PauseIndexReplicationAction
import com.amazon.elasticsearch.replication.action.pause.PauseIndexReplicationRequest
import com.amazon.elasticsearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import com.amazon.elasticsearch.replication.metadata.ReplicationMetadataManager
import com.amazon.elasticsearch.replication.metadata.ReplicationOverallState
import com.amazon.elasticsearch.replication.metadata.state.getReplicationStateParamsForIndex
import com.amazon.elasticsearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import com.amazon.elasticsearch.replication.task.CrossClusterReplicationTask
import com.amazon.elasticsearch.replication.task.ReplicationState
import com.amazon.elasticsearch.replication.util.indicesService
import com.amazon.elasticsearch.replication.util.stackTraceToString
import com.amazon.elasticsearch.replication.util.suspendExecute
import com.amazon.elasticsearch.replication.util.suspendExecuteWithRetries
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
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
import org.elasticsearch.tasks.TaskId
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.NodeNotConnectedException

class ShardReplicationTask(id: Long, type: String, action: String, description: String, parentTask: TaskId,
                           params: ShardReplicationParams, executor: String, clusterService: ClusterService,
                           threadPool: ThreadPool, client: Client, replicationMetadataManager: ReplicationMetadataManager,
                           replicationSettings: ReplicationSettings)
    : CrossClusterReplicationTask(id, type, action, description, parentTask, emptyMap(),
                                  executor, clusterService, threadPool, client, replicationMetadataManager, replicationSettings) {

    override val remoteCluster: String = params.remoteCluster
    override val followerIndexName: String = params.followerShardId.indexName
    private val remoteShardId = params.remoteShardId
    private val followerShardId = params.followerShardId
    private val remoteClient = client.getRemoteClusterClient(remoteCluster)
    private val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(clusterService.clusterName.value(), remoteClient)
    private var paused = false
    val backOffForNodeDiscovery = 1000L

    private val clusterStateListenerForTaskInterruption = ClusterStateListenerForTaskInterruption()

    override val log = Loggers.getLogger(javaClass, followerShardId)!!

    companion object {
        fun taskIdForShard(shardId: ShardId) = "replication:${shardId}"
    }

    @ObsoleteCoroutinesApi
    override suspend fun execute(initialState: PersistentTaskState?) {
        replicate()
    }

    override suspend fun cleanup() {
        /* This is to minimise overhead of calling an additional listener as
        * it continues to be called even after the task is completed.
         */
        clusterService.removeListener(clusterStateListenerForTaskInterruption)
        if (paused) {
            log.debug("Pausing and not removing lease for index $followerIndexName and shard $followerShardId task")
            return
        }
        retentionLeaseHelper.removeRetentionLease(remoteShardId, followerShardId)

    }

    private fun addListenerToInterruptTask() {
        clusterService.addListener(clusterStateListenerForTaskInterruption)
    }

    inner class ClusterStateListenerForTaskInterruption : ClusterStateListener {
        override fun clusterChanged(event: ClusterChangedEvent) {
            log.debug("Cluster metadata listener invoked on shard task...")
            if (event.metadataChanged()) {
                val replicationStateParams = getReplicationStateParamsForIndex(clusterService, followerShardId.indexName)
                if (replicationStateParams == null) {
                    if (PersistentTasksNodeService.Status(State.STARTED) == status)
                        scope.cancel("Shard replication task received an interrupt.")
                } else if (replicationStateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE] == ReplicationOverallState.PAUSED.name){
                    log.info("Pause state received for index $followerIndexName. Cancelling $followerShardId task")
                    paused = true
                    scope.cancel("Shard replication task received pause.")
                }
            }
        }
    }

    override fun indicesOrShards() = listOf(followerShardId)

    @ObsoleteCoroutinesApi
    private suspend fun replicate() {
        updateTaskState(FollowingState)
        val followerIndexService = indicesService.indexServiceSafe(followerShardId.index)
        val indexShard = followerIndexService.getShard(followerShardId.id)
        // Adding retention lease at local checkpoint of a node. This makes sure
        // new tasks spawned after node changes/shard movements are handled properly
        log.info("Adding retentionlease at follower sequence number: ${indexShard.lastSyncedGlobalCheckpoint}")
        retentionLeaseHelper.addRetentionLease(remoteShardId, indexShard.lastSyncedGlobalCheckpoint , followerShardId)
        addListenerToInterruptTask()

        // Since this setting is not dynamic, setting update would only reflect after pause-resume or on a new replication job.
        val rateLimiter = Semaphore(replicationSettings.readersPerShard)
        val sequencer = TranslogSequencer(scope, replicationMetadata, followerShardId, remoteCluster, remoteShardId.indexName,
                                          TaskId(clusterService.nodeName, id), client, indexShard.localCheckpoint)

        val changeTracker = ShardReplicationChangesTracker(indexShard, replicationSettings)

        coroutineScope {
            while (scope.isActive) {
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
                    } catch (e: ElasticsearchTimeoutException) {
                        logInfo("Timed out waiting for new changes. Current seqNo: $fromSeqNo. $e")
                        changeTracker.updateBatchFetched(false, fromSeqNo, toSeqNo, fromSeqNo - 1,-1)
                    } catch (e: NodeNotConnectedException) {
                        logInfo("Node not connected. Retrying request using a different node. ${e.stackTraceToString()}")
                        delay(backOffForNodeDiscovery)
                        changeTracker.updateBatchFetched(false, fromSeqNo, toSeqNo, fromSeqNo - 1,-1)
                    } catch (e: Exception) {
                        logInfo("Unable to get changes from seqNo: $fromSeqNo. ${e.stackTraceToString()}")
                        changeTracker.updateBatchFetched(false, fromSeqNo, toSeqNo, fromSeqNo - 1,-1)
                    } finally {
                        rateLimiter.release()
                    }
                }

                //renew retention lease with global checkpoint so that any shard that picks up shard replication task has data until then.
                try {
                    retentionLeaseHelper.renewRetentionLease(remoteShardId, indexShard.lastSyncedGlobalCheckpoint, followerShardId)
                } catch (ex: Exception) {
                    when (ex) {
                        is RetentionLeaseInvalidRetainingSeqNoException, is RetentionLeaseNotFoundException -> {
                            throw ex
                        }
                        else -> log.info("Exception renewing retention lease. Not an issue", ex);
                    }
                }
            }
        }
        sequencer.close()
    }

    private suspend fun getChanges(fromSeqNo: Long, toSeqNo: Long): GetChangesResponse {
        val remoteClient = client.getRemoteClusterClient(remoteCluster)
        val request = GetChangesRequest(remoteShardId, fromSeqNo, toSeqNo)
        return remoteClient.suspendExecuteWithRetries(replicationMetadata = replicationMetadata,
                action = GetChangesAction.INSTANCE, req = request, log = log)
    }
    private fun logDebug(msg: String) {
        log.debug("${Thread.currentThread().name}: $msg")
    }
    private fun logInfo(msg: String) {
        log.info("${Thread.currentThread().name}: $msg")
    }

    override fun toString(): String {
        return "ShardReplicationTask(from=${remoteCluster}$remoteShardId to=$followerShardId)"
    }

    override fun replicationTaskResponse(): CrossClusterReplicationTaskResponse {
        // Cancellation and valid executions are marked as completed
        return CrossClusterReplicationTaskResponse(ReplicationState.COMPLETED.name)
    }

    // ToDo : Use in case of non retriable errors
    private suspend fun pauseReplicationTasks(reason: String) {
        val pauseReplicationResponse = client.suspendExecute(PauseIndexReplicationAction.INSTANCE,
                PauseIndexReplicationRequest(followerIndexName, reason))
        if (!pauseReplicationResponse.isAcknowledged)
            throw ReplicationException("Failed to pause replication")
    }
}
