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
import com.amazon.elasticsearch.replication.util.suspending
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.ElasticsearchTimeoutException
import org.elasticsearch.action.NoSuchNodeException
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.TransportActions
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.ClusterChangedEvent
import org.elasticsearch.cluster.ClusterStateListener
import org.elasticsearch.cluster.node.DiscoveryNode
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.index.seqno.RetentionLeaseActions
import org.elasticsearch.index.seqno.RetentionLeaseInvalidRetainingSeqNoException
import org.elasticsearch.index.seqno.RetentionLeaseNotFoundException
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.index.shard.ShardNotFoundException
import org.elasticsearch.persistent.PersistentTaskState
import org.elasticsearch.persistent.PersistentTasksNodeService
import org.elasticsearch.tasks.TaskId
import org.elasticsearch.threadpool.ThreadPool
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import org.elasticsearch.transport.NodeNotConnectedException

class ShardReplicationTask(id: Long, type: String, action: String, description: String, parentTask: TaskId,
                           params: ShardReplicationParams, executor: String, clusterService: ClusterService,
                           threadPool: ThreadPool, client: Client, replicationMetadataManager: ReplicationMetadataManager,
                           private val translogBuffer: AtomicLong,
                           private val translogBufferMutex: Mutex,
                           private val batchSizeEstimate: ConcurrentHashMap<String, Long>,
                           private val translogBufferNew: TranslogBuffer)
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
    @Volatile private var isFirstFetch = true

    private val clusterStateListenerForTaskInterruption = ClusterStateListenerForTaskInterruption()

    @Volatile private var batchSize = clusterService.clusterSettings.get(REPLICATION_CHANGE_BATCH_SIZE)
    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(REPLICATION_CHANGE_BATCH_SIZE) { batchSize = it }
    }

    override val log = Loggers.getLogger(javaClass, followerShardId)!!

    companion object {
        fun taskIdForShard(shardId: ShardId) = "replication:${shardId}"
        const val CONCURRENT_REQUEST_RATE_LIMIT = 2
    }

    @ObsoleteCoroutinesApi
    override suspend fun execute(initialState: PersistentTaskState?) {
        replicate()
    }

    override suspend fun cleanup() {
        postErrorBufferUpdate()
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

    private suspend fun preFetchBufferUpdate() {
        // If this is the first fetch for the index, then ony one thread of execution is allowed to proceed to
        // fetch the translog batch. After the first fetch is complete, any number of shards of an index can fetch
        // translog batches in parallel, provided there is enough space in the buffer 'translogBuffer'.
        // Important point to note: The first fetch doesn't check buffer size and can happen even if the buffer is
        // full (by another index).

        val indexBatchSizeEstimate: Long
        translogBufferMutex.lock()

        // If it is the first fetch, we hold the lock, else we release it.
        if (batchSizeEstimate.containsKey(followerIndexName)) {
            indexBatchSizeEstimate = batchSizeEstimate[followerIndexName]!!
            log.info("First fetch for index $followerIndexName already finished. " +
                    "Batch size guess is ${indexBatchSizeEstimate.toDouble()/1024/1024} MB.")
            isFirstFetch = false
            translogBufferMutex.unlock() // unlocking now and re-locking again in while loop
            while (true) {
                translogBufferMutex.lock()
                val currBufferSize = translogBuffer.get()
                if (currBufferSize <= indexBatchSizeEstimate) {
                    // Buffer is low on free memory
                    translogBufferMutex.unlock()
                    delay(500)
                } else {
                    val bufferSize = translogBuffer.get()
                    val updatedBufferSize = translogBuffer.addAndGet(-1 * indexBatchSizeEstimate)
                    log.info("Translog buffer size updated from ${bufferSize.toDouble()/1024/1024} MB to " +
                            "${updatedBufferSize.toDouble()/1024/1024} MB after estimating that batch would be " +
                            "of size ${indexBatchSizeEstimate.toDouble()/1024/1024} MB.")
                    translogBufferMutex.unlock()
                    break
                }
            }
        }
    }

    private suspend fun reFillBuffer() {
        translogBufferMutex.lock()
        val guess = batchSizeEstimate[followerIndexName]!!
        val currBufferSize = translogBuffer.addAndGet(guess)
        log.info("Guessed size of batch was ${guess.toDouble()/1024/1024} MB, so updated buffer back by same value. " +
                "Buffer is now ${currBufferSize.toDouble()/1024/1024} MB.")
        translogBufferMutex.unlock()
    }

    override fun indicesOrShards() = listOf(followerShardId)

    private suspend fun postFetchBufferUpdate(changesResponse: GetChangesResponse) {
        if (isFirstFetch) {
            // TODO: don't wait for first fetch to be written, before letting fetching happen in
            //  parallel?
            val perOperationSize = changesResponse.changesSizeEstimate/changesResponse.changes.size
            batchSizeEstimate[followerIndexName] = perOperationSize.toInt().times(
                    clusterService.clusterSettings.get(REPLICATION_CHANGE_BATCH_SIZE)).toLong()
            log.info("First fetch completed. Batch size is " +
                    "${changesResponse.changesSizeEstimate.toDouble()/1024/1024} MB and # of operations " +
                    "are ${changesResponse.changes.size}, so setting buffer size to " +
                    "${batchSizeEstimate[followerIndexName]}/1024/1024 MB.")
            translogBufferMutex.unlock()
        } else {
            reFillBuffer()
        }
    }

    private suspend fun postErrorBufferUpdate() {
        if (isFirstFetch && translogBufferMutex.isLocked) {
            translogBufferMutex.unlock()
        } else {
            reFillBuffer()
        }
    }

    @ObsoleteCoroutinesApi
    private suspend fun replicate() {
        updateTaskState(FollowingState)
        log.info("Starting with translog buffer size ${translogBuffer.get().toDouble()/1024/1024} MB")
        val followerIndexService = indicesService.indexServiceSafe(followerShardId.index)
        val indexShard = followerIndexService.getShard(followerShardId.id)
        // Adding retention lease at local checkpoint of a node. This makes sure
        // new tasks spawned after node changes/shard movements are handled properly
        log.info("Adding retentionlease at follower sequence number: ${indexShard.lastSyncedGlobalCheckpoint}")
        retentionLeaseHelper.addRetentionLease(remoteShardId, indexShard.lastSyncedGlobalCheckpoint , followerShardId)

        addListenerToInterruptTask()

        // Not really used yet as we only have one get changes action at a time.
        val rateLimiter = Semaphore(CONCURRENT_REQUEST_RATE_LIMIT)
        var seqNo = indexShard.localCheckpoint + 1
        val sequencer = TranslogSequencer(scope, replicationMetadata, followerShardId, remoteCluster, remoteShardId.indexName,
                                          TaskId(clusterService.nodeName, id), client, rateLimiter, seqNo - 1)

        // TODO: Redesign this to avoid sharing the rateLimiter between this block and the sequencer.
        //       This was done as a stopgap to work around a concurrency bug that needed to be fixed fast.
        coroutineScope {
            while (scope.isActive) {
                launch {
                    log.info("coroutine got launched")
                    rateLimiter.acquire()
                    log.info("coroutine past reatelimiter")
                    try {
                        preFetchBufferUpdate()
                        val startTime = System.nanoTime()
                        val changesResponse = getChanges(seqNo)
                        val endTime = System.nanoTime()
                        log.info("Got ${changesResponse.changes.size} changes starting from seqNo: $seqNo in " +
                                "${(endTime - startTime)/1000000} ms")
                        sequencer.send(changesResponse)
                        seqNo = changesResponse.changes.lastOrNull()?.seqNo()?.inc() ?: seqNo
                        postFetchBufferUpdate(changesResponse)
                    } catch (e: ElasticsearchTimeoutException) {
                        log.info("Timed out waiting for new changes. Current seqNo: $seqNo")
                        postErrorBufferUpdate()
                        rateLimiter.release()
                    } catch (e: NodeNotConnectedException) {
                        log.info("Node not connected. Retrying request using a different node. $e")
                        delay(backOffForNodeDiscovery)
                        rateLimiter.release()
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
        sequencer.close()
    }

    private suspend fun getChanges(fromSeqNo: Long): GetChangesResponse {
        val remoteClient = client.getRemoteClusterClient(remoteCluster)
        val request = GetChangesRequest(remoteShardId, fromSeqNo, fromSeqNo + batchSize)
        return remoteClient.suspendExecuteWithRetries(replicationMetadata = replicationMetadata,
                action = GetChangesAction.INSTANCE, req = request, log = log)
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
