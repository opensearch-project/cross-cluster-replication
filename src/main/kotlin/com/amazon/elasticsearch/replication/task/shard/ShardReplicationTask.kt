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
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import org.elasticsearch.transport.NodeNotConnectedException
import java.util.concurrent.TimeoutException

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

//    val bleh = params.followerShardId.
    private val PARALLEL_FETCHES = 5

    // TODO finish this comment
    // When we perform fetch of translog batch from leader, we do so by blocking buffer. TODO: finish this comment
    private var lastFetchTimedOut = ConcurrentHashMap<Int, Boolean>()

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
        // TODO: see if this is required really
        // postErrorBufferUpdate()
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

    private suspend fun preFetchBufferUpdate(): Boolean {
        // If this is the first fetch for the index, then ony one thread of execution is allowed to proceed to
        // fetch the translog batch. After the first fetch is complete, any number of shards of an index can fetch
        // translog batches in parallel, provided there is enough space in the buffer 'translogBuffer'.
        // Important point to note: The first fetch doesn't check buffer size and can happen even if the buffer is
        // full (by another index).

        log.info("getting batch size or locking if first fetch")
        val batchSizeEstimate = translogBufferNew.getBatchSizeEstimateOrLockIfFirstFetch(followerIndexName)
        if (batchSizeEstimate == translogBufferNew.FIRST_FETCH) {
            log.info("first fetch")
            return false
        }
        log.info("not first fetch")
        isFirstFetch = false

        val maxDelayBeforeWarnLog = 60_000L         // 60 seconds
        val maxDelayBeforeError = 10 * 60_000L      // 10 minutes
        val delayInterval = 500L                    // 500ms
        var totalDelay = 0L

        while (true) {
            log.info("adding batch to buffer")
            val (ok, isInactive) = translogBufferNew.addBatch(followerIndexName)
            log.info("adding batch to buffer done with output ok? $ok")
            if (ok) {
                return isInactive
            }
            if (totalDelay >= maxDelayBeforeWarnLog) {
                log.warn("Not able to consume buffer to fetch translog batch for ${totalDelay/1000}s now.")
            }
            if (totalDelay >= maxDelayBeforeError) {
                log.warn("Not able to consume buffer to fetch translog batch for ${totalDelay/1000}s. Giving up..")
                throw TimeoutException()
            }
            delay(delayInterval)
            totalDelay += delayInterval
        }
    }

    // TODO: isIndexInactive/indexInactiveCurrently aren't intuitive. Come up with a better name
    private suspend fun postFetchBufferUpdate(changesResponse: GetChangesResponse, isIndexInactive: Boolean) {
        log.info("postupdate invoked")
        if (isFirstFetch) {
            val perOperationSize = (changesResponse.changesSizeEstimate/changesResponse.changes.size).toInt()
            val estimate = perOperationSize.times(clusterService.clusterSettings.get(REPLICATION_CHANGE_BATCH_SIZE)).toLong()
            translogBufferNew.addEstimateAndUnlock(followerIndexName, estimate)
            log.info("first fetch finished and unlocked")
        } else {
            translogBufferNew.removeBatch(followerIndexName, false, isIndexInactive)
        }
        log.info("postupdate finished")
    }

    private suspend fun postErrorBufferUpdate(markIndexInactive: Boolean, indexInactiveCurrently: Boolean) {
        log.info("postupdate error involed")
        // We keep the mutex locked for extended periods of time only for the first fetch, so check locking only in the case
        if (isFirstFetch) {
            translogBufferNew.unlockIfLocked()
        } else {
            // Only when it is NOT the first fetch do we need to reset the index in buffer.
            /// TODO update this comment
            translogBufferNew.removeBatch(followerIndexName, markIndexInactive, indexInactiveCurrently)
        }
        log.info("postupdate error finished")
    }

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

        // TODO: make atomic, as updated by many threads?
        var seqNo = indexShard.localCheckpoint + 1
        val sequencer = TranslogSequencer(scope, replicationMetadata, followerShardId, remoteCluster, remoteShardId.indexName,
                                          TaskId(clusterService.nodeName, id), client, seqNo - 1)


        coroutineScope {
            for (i in 1..PARALLEL_FETCHES) {
                launch {
                    while (true) {
                        log.info("coroutine got launched")
                        var markIndexInactive = false
                        var timeOutEncountered = false
                        var indexInactiveCurrently = false
                        var fetchSuccessful = false
                        try {
                            indexInactiveCurrently = preFetchBufferUpdate()
                            val startTime = System.nanoTime()
                            val changesResponse = getChanges(seqNo)
                            val endTime = System.nanoTime()
                            log.info("Got ${changesResponse.changes.size} changes starting from seqNo: $seqNo in " +
                                    "${(endTime - startTime)/1000000} ms")
                            sequencer.send(changesResponse)
                            seqNo = changesResponse.changes.lastOrNull()?.seqNo()?.inc() ?: seqNo
                            fetchSuccessful = true
                            postFetchBufferUpdate(changesResponse, indexInactiveCurrently)
                        } catch (e: ElasticsearchTimeoutException) {
                            log.info("Timed out waiting for new changes. Current seqNo: $seqNo")
                            // TODO: move inside posterrorbufferudpate?
                            timeOutEncountered = true
                            if (!lastFetchTimedOut.contains(i)) {
                                lastFetchTimedOut[i] = true
                            }
                            var numTimeOuts = 0
                            val it = lastFetchTimedOut.elements().asIterator()
                            while(it.hasNext()) {
                                val value = it.next()
                                if (value) {
                                    numTimeOuts++
                                }
                            }
                            if (numTimeOuts == PARALLEL_FETCHES) {
                                markIndexInactive = true
                            }
                        } catch (e: NodeNotConnectedException) {
                            log.info("Node not connected. Retrying request using a different node. $e")
                            delay(backOffForNodeDiscovery)
                        } finally {
                            lastFetchTimedOut[i] = timeOutEncountered
                            if (!fetchSuccessful) {
                                postErrorBufferUpdate(markIndexInactive, indexInactiveCurrently)
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
