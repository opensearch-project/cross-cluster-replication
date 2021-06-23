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
import com.amazon.elasticsearch.replication.action.changes.GetChangesAction
import com.amazon.elasticsearch.replication.action.changes.GetChangesRequest
import com.amazon.elasticsearch.replication.action.changes.GetChangesResponse
import com.amazon.elasticsearch.replication.action.pause.PauseIndexReplicationAction
import com.amazon.elasticsearch.replication.action.pause.PauseIndexReplicationRequest
import com.amazon.elasticsearch.replication.metadata.REPLICATION_OVERALL_STATE_KEY
import com.amazon.elasticsearch.replication.metadata.REPLICATION_OVERALL_STATE_PAUSED
import com.amazon.elasticsearch.replication.metadata.getReplicationStateParamsForIndex
import com.amazon.elasticsearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import com.amazon.elasticsearch.replication.task.CrossClusterReplicationTask
import com.amazon.elasticsearch.replication.task.ReplicationState
import com.amazon.elasticsearch.replication.util.indicesService
import com.amazon.elasticsearch.replication.util.suspendExecute
import com.amazon.elasticsearch.replication.util.suspendExecuteWithRetries
import com.amazon.elasticsearch.replication.util.suspending
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.cancel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.sync.Semaphore
import org.elasticsearch.ElasticsearchTimeoutException
import org.elasticsearch.action.NoSuchNodeException
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.ClusterChangedEvent
import org.elasticsearch.cluster.ClusterStateListener
import org.elasticsearch.cluster.node.DiscoveryNode
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.index.seqno.RetentionLeaseActions
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.index.shard.ShardNotFoundException
import org.elasticsearch.persistent.PersistentTaskState
import org.elasticsearch.persistent.PersistentTasksNodeService
import org.elasticsearch.tasks.TaskId
import org.elasticsearch.threadpool.ThreadPool

class ShardReplicationTask(id: Long, type: String, action: String, description: String, parentTask: TaskId,
                           params: ShardReplicationParams, executor: String, clusterService: ClusterService,
                           threadPool: ThreadPool, client: Client)
    : CrossClusterReplicationTask(id, type, action, description, parentTask, emptyMap(),
                                  executor, clusterService, threadPool, client) {

    override val remoteCluster: String = params.remoteCluster
    override val followerIndexName: String = params.followerShardId.indexName
    private val remoteShardId = params.remoteShardId
    private val followerShardId = params.followerShardId
    private val remoteClient = client.getRemoteClusterClient(remoteCluster)
    private val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(clusterService.clusterName.value(), remoteClient)
    private var paused = false

    private val clusterStateListenerForTaskInterruption = ClusterStateListenerForTaskInterruption()

    @Volatile private var batchSize = clusterService.clusterSettings.get(REPLICATION_CHANGE_BATCH_SIZE)
    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(REPLICATION_CHANGE_BATCH_SIZE) { batchSize = it }
    }

    override val log = Loggers.getLogger(javaClass, followerShardId)!!

    companion object {
        fun taskIdForShard(shardId: ShardId) = "replication:${shardId}"
        const val CONCURRENT_REQUEST_RATE_LIMIT = 10
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
                } else if (replicationStateParams[REPLICATION_OVERALL_STATE_KEY] == REPLICATION_OVERALL_STATE_PAUSED){
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

        val node = primaryShardNode()

        // TODO: Acquire retention lease prior to initiating remote recovery
        retentionLeaseHelper.addRetentionLease(remoteShardId, seqNo, followerShardId)

        addListenerToInterruptTask()

        // Not really used yet as we only have one get changes action at a time.
        val rateLimiter = Semaphore(CONCURRENT_REQUEST_RATE_LIMIT)
        var seqNo = indexShard.localCheckpoint + 1
        val sequencer = TranslogSequencer(scope, followerShardId, remoteCluster, remoteShardId.indexName,
                                          TaskId(clusterService.nodeName, id), client, rateLimiter, seqNo - 1)

        // TODO: Redesign this to avoid sharing the rateLimiter between this block and the sequencer.
        //       This was done as a stopgap to work around a concurrency bug that needed to be fixed fast.
        while (scope.isActive) {
            rateLimiter.acquire()
            try {
                val changesResponse = getChanges(node, seqNo)
                log.info("Got ${changesResponse.changes.size} changes starting from seqNo: $seqNo")
                sequencer.send(changesResponse)
                seqNo = changesResponse.changes.lastOrNull()?.seqNo()?.inc() ?: seqNo
            } catch (e: ElasticsearchTimeoutException) {
                log.info("Timed out waiting for new changes. Current seqNo: $seqNo")
                rateLimiter.release()
                continue
            }
            //renew retention lease with global checkpoint so that any shard that picks up shard replication task has data until then.
            retentionLeaseHelper.renewRetentionLease(remoteShardId, indexShard.lastSyncedGlobalCheckpoint, followerShardId)
        }
        sequencer.close()
    }

    private suspend fun primaryShardNode(): DiscoveryNode {
        val clusterStateRequest = remoteClient.admin().cluster().prepareState()
            .clear()
            .setIndices(remoteShardId.indexName)
            .setRoutingTable(true)
            .setNodes(true)
            .setIndicesOptions(IndicesOptions.strictSingleIndexNoExpandForbidClosed())
            .request()
        val remoteState = suspending(remoteClient.admin().cluster()::state)(clusterStateRequest).state
        val shardRouting = remoteState.routingNodes.activePrimary(remoteShardId)
            ?: throw ShardNotFoundException(remoteShardId, "cluster: $remoteCluster")
        return remoteState.nodes().get(shardRouting.currentNodeId())
            ?: throw NoSuchNodeException("remote: $remoteCluster:${shardRouting.currentNodeId()}")
    }

    private suspend fun getChanges(remoteNode: DiscoveryNode, fromSeqNo: Long): GetChangesResponse {
        val remoteClient = client.getRemoteClusterClient(remoteCluster)
        val request = GetChangesRequest(remoteNode, remoteShardId, fromSeqNo, fromSeqNo + batchSize)
        return remoteClient.suspendExecuteWithRetries(action = GetChangesAction.INSTANCE, req = request, log = log)
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
        val pauseReplicationResponse = client.suspendExecute(PauseIndexReplicationAction.INSTANCE, PauseIndexReplicationRequest(followerIndexName, reason))
        if (!pauseReplicationResponse.isAcknowledged)
            throw ReplicationException("Failed to pause replication")
    }
}
