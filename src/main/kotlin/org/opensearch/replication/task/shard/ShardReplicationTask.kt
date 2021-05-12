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

import org.opensearch.replication.ReplicationPlugin.Companion.REPLICATION_CHANGE_BATCH_SIZE
import org.opensearch.replication.action.changes.GetChangesAction
import org.opensearch.replication.action.changes.GetChangesRequest
import org.opensearch.replication.action.changes.GetChangesResponse
import org.opensearch.replication.metadata.getReplicationStateParamsForIndex
import org.opensearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import org.opensearch.replication.task.CrossClusterReplicationTask
import org.opensearch.replication.task.ReplicationState
import org.opensearch.replication.util.indicesService
import org.opensearch.replication.util.suspending
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.cancel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.sync.Semaphore
import org.opensearch.OpenSearchTimeoutException
import org.opensearch.action.NoSuchNodeException
import org.opensearch.action.support.IndicesOptions
import org.opensearch.client.Client
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.logging.Loggers
import org.opensearch.index.seqno.RetentionLeaseActions
import org.opensearch.index.shard.ShardId
import org.opensearch.index.shard.ShardNotFoundException
import org.opensearch.persistent.PersistentTaskState
import org.opensearch.persistent.PersistentTasksNodeService
import org.opensearch.replication.util.suspendExecuteWithRetries
import org.opensearch.tasks.TaskId
import org.opensearch.threadpool.ThreadPool

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
        retentionLeaseHelper.removeRetentionLease(remoteShardId, followerShardId)
        /* This is to minimise overhead of calling an additional listener as
         * it continues to be called even after the task is completed.
         */
        clusterService.removeListener(clusterStateListenerForTaskInterruption)
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
                }
            }
        }
    }

    override fun indicesOrShards() = listOf(followerShardId)

    @ObsoleteCoroutinesApi
    private suspend fun replicate() {
        updateTaskState(FollowingState)
        // TODO: Acquire retention lease prior to initiating remote recovery
        retentionLeaseHelper.addRetentionLease(remoteShardId, RetentionLeaseActions.RETAIN_ALL, followerShardId)
        val followerIndexService = indicesService.indexServiceSafe(followerShardId.index)
        val indexShard = followerIndexService.getShard(followerShardId.id)
        // After restore, persisted localcheckpoint is matched with maxSeqNo.
        // Fetch the operations after localCheckpoint from the leader
        var seqNo = indexShard.localCheckpoint + 1
        val node = primaryShardNode()
        addListenerToInterruptTask()

        // Not really used yet as we only have one get changes action at a time.
        val rateLimiter = Semaphore(CONCURRENT_REQUEST_RATE_LIMIT)
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
            } catch (e: OpenSearchTimeoutException) {
                log.info("Timed out waiting for new changes. Current seqNo: $seqNo")
                rateLimiter.release()
                continue
            }
            retentionLeaseHelper.renewRetentionLease(remoteShardId, seqNo, followerShardId)
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
}
