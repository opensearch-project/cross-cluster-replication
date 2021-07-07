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
import com.amazon.elasticsearch.replication.ReplicationPlugin.Companion.REPLICATION_TRANSLOG_BUFFER_PERCENT
import com.amazon.elasticsearch.replication.ReplicationPlugin.Companion.REPLICATION_TRANSLOG_FETCH_PARALLELISM
import com.amazon.elasticsearch.replication.TranslogBuffer
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
import com.amazon.elasticsearch.replication.util.suspendExecute
import com.amazon.elasticsearch.replication.util.suspendExecuteWithRetries
import kotlinx.coroutines.*
import org.elasticsearch.ElasticsearchTimeoutException
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.ClusterChangedEvent
import org.elasticsearch.cluster.ClusterStateListener
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.lease.Releasable
import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException
import org.elasticsearch.index.seqno.RetentionLeaseInvalidRetainingSeqNoException
import org.elasticsearch.index.seqno.RetentionLeaseNotFoundException
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.monitor.jvm.JvmInfo
import org.elasticsearch.persistent.PersistentTaskState
import org.elasticsearch.persistent.PersistentTasksNodeService
import org.elasticsearch.tasks.TaskId
import org.elasticsearch.threadpool.ThreadPool
import java.util.concurrent.ConcurrentHashMap
import org.elasticsearch.transport.NodeNotConnectedException
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

class ShardReplicationTask(id: Long, type: String, action: String, description: String, parentTask: TaskId,
                           params: ShardReplicationParams, executor: String, clusterService: ClusterService,
                           threadPool: ThreadPool, client: Client, replicationMetadataManager: ReplicationMetadataManager,
                           private val translogBuffer: TranslogBuffer)
    : CrossClusterReplicationTask(id, type, action, description, parentTask, emptyMap(),
                                  executor, clusterService, threadPool, client, replicationMetadataManager) {

    override val remoteCluster: String = params.remoteCluster
    override val followerIndexName: String = params.followerShardId.indexName
    private val remoteShardId = params.remoteShardId
    private val followerShardId = params.followerShardId
    private val remoteClient = client.getRemoteClusterClient(remoteCluster)
    private val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(clusterService.clusterName.value(), remoteClient)
    private var paused = false
    val backOffForNodeDiscovery = 1000L

    private val PARALLEL_FETCHES = 5

    private val clusterStateListenerForTaskInterruption = ClusterStateListenerForTaskInterruption()

    @Volatile private var batchSize = clusterService.clusterSettings.get(REPLICATION_CHANGE_BATCH_SIZE)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(REPLICATION_CHANGE_BATCH_SIZE) { batchSize = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(REPLICATION_TRANSLOG_BUFFER_PERCENT) { translogBuffer.updateBufferSize(it) }
        clusterService.clusterSettings.addSettingsUpdateConsumer(REPLICATION_TRANSLOG_FETCH_PARALLELISM) { translogBuffer.updateParallelism(it) }
    }

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

        val seqNo = AtomicLong(indexShard.localCheckpoint + 1)
        val sequencer = TranslogSequencer(scope, replicationMetadata, followerShardId, remoteCluster, remoteShardId.indexName,
                                          TaskId(clusterService.nodeName, id), client, seqNo.get() - 1, translogBuffer)

        coroutineScope {
            for (i in 1..PARALLEL_FETCHES) {
                launch {
                    delay((Math.random()*10_000).toLong()) // random initial jitter of up to 10 seconds
                    while (true) {
                        log.info("Fetching translog batch from leader...")
                        var fetchSuccess = false
                        try {
                            translogBuffer.acquireRateLimiter(followerShardId)

                            val startTime = System.nanoTime()
                            val changesResponse = getChanges(seqNo.get())
                            val endTime = System.nanoTime()
                            log.info("Got ${changesResponse.changes.size} changes starting from seqNo: $seqNo in " +
                                    "${(endTime - startTime)/1000000} ms")
                            val closeable = translogBuffer.markBatchAdded(changesResponse.changesSizeEstimate)
                            sequencer.send(changesResponse, closeable)
                            val newSeqNo = changesResponse.changes.lastOrNull()?.seqNo()?.inc() ?: seqNo
                            seqNo.getAndSet(newSeqNo.toLong())
                            fetchSuccess = true
                        } catch (e: EsRejectedExecutionException) {
                            log.info("Buffer full. Retrying")
                        } catch (e: ElasticsearchTimeoutException) {
                            log.info("Timed out waiting for new changes. Current seqNo: $seqNo")
                        } catch (e: NodeNotConnectedException) {
                            log.info("Node not connected. Retrying request using a different node. $e")
                            delay(backOffForNodeDiscovery)
                        } finally {
                            translogBuffer.releaseRateLimiter(followerShardId, fetchSuccess)
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
            }
        }
        sequencer.close()
    }

    private suspend fun getChanges(fromSeqNo: Long): GetChangesResponse {
        val remoteClient = client.getRemoteClusterClient(remoteCluster)
        val request = GetChangesRequest(remoteShardId, fromSeqNo, fromSeqNo + batchSize)
        return remoteClient.suspendExecuteWithRetries(replicationMetadata = replicationMetadata,
                 action = GetChangesAction.INSTANCE, req = request, log = log, maxTimeOut = 10_000)
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
