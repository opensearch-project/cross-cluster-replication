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
    @Volatile private var isFirstFetch = AtomicBoolean(true)
    @Volatile private var isFirstFetchStarted = AtomicBoolean(false)

    private val PARALLEL_FETCHES = 5

    // When we perform fetch of translog batch from leader, we do so by blocking buffer. Now when replication of a shard
    // finishes, the request to fetch batch keeps blocking for 60 seconds and then gives up.
    // Map to keep track of how many of the parallel fetches experienced timeouts when making call to fetch translog. An
    // example when timeout happens is when replication has finished on the follower
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

        val batchSizeEstimate = translogBuffer.getBatchSizeEstimateOrLockIfFirstFetch(followerIndexName)
        if (batchSizeEstimate == translogBuffer.FIRST_FETCH) {
            isFirstFetchStarted.set(true)
            log.info("First fetch started for index $followerIndexName.")
            return false
        }
        isFirstFetch.set(false)

        val maxDelayBeforeWarnLog = 60_000L         // 60 seconds
        val maxDelayBeforeError = 10 * 60_000L      // 10 minutes
        val delayInterval = 500L                    // 500ms
        var totalDelay = 0L

        while (true) {
            val (ok, isInactive) = translogBuffer.addBatch(followerIndexName, followerShardId.toString())
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

    private suspend fun postFetchBufferUpdate(changesResponse: GetChangesResponse, inactiveWhenBatchAdded: Boolean) {
        if (isFirstFetch.get()) {
            val perOperationSize = (changesResponse.changesSizeEstimate/changesResponse.changes.size).toInt()
            val estimate = perOperationSize.times(clusterService.clusterSettings.get(REPLICATION_CHANGE_BATCH_SIZE)).toLong()
            translogBuffer.addEstimateAfterFirstFetchAndUnlock(followerIndexName, estimate)
        } else {
            translogBuffer.removeBatch(followerIndexName, followerShardId.toString(), false, inactiveWhenBatchAdded)
        }
    }

    private suspend fun postErrorBufferUpdate(markShardInactive: Boolean, inactiveWhenBatchAdded: Boolean) {
        // We keep the mutex locked for extended periods of time only for the first fetch, so check locking only in
        // that case. Once isFirstFetch is set to false, all accesses to translogBuffer's mutex happens inside a
        // `withLock` construct, which automatically takes care of unlocking in case of errors, exceptions and
        // cancellations.
        if (isFirstFetch.get() && isFirstFetchStarted.get()) {
            // With the above two conditions, we can be very confident that the first fetch has now started, and the lock, if
            // acquired, is acquired by the current shard's first fetch only
            translogBuffer.unlockIfLocked()
        } else {
            translogBuffer.removeBatch(followerIndexName, followerShardId.toString(), markShardInactive, inactiveWhenBatchAdded)
        }
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

        val seqNo = AtomicLong(indexShard.localCheckpoint + 1)
        val sequencer = TranslogSequencer(scope, replicationMetadata, followerShardId, remoteCluster, remoteShardId.indexName,
                                          TaskId(clusterService.nodeName, id), client, seqNo.get() - 1)

        coroutineScope {
            for (i in 1..PARALLEL_FETCHES) {
                launch {
                    while (true) {
                        log.info("Fetching translog batch from leader...")
                        // We mark shard inactive when we have seen a timeout from all the parallel coroutines for the current shard
                        var markShardInactive = false
                        var timeOutEncountered = false
                        var shardInactiveWhenBatchAdded = false
                        var fetchSuccessful = false
                        try {
                            shardInactiveWhenBatchAdded = preFetchBufferUpdate()
                            val startTime = System.nanoTime()
                            val changesResponse = getChanges(seqNo.get())
                            val endTime = System.nanoTime()
                            log.info("Got ${changesResponse.changes.size} changes starting from seqNo: $seqNo in " +
                                    "${(endTime - startTime)/1000000} ms")
                            sequencer.send(changesResponse)
                            val newSeqNo = changesResponse.changes.lastOrNull()?.seqNo()?.inc() ?: seqNo
                            seqNo.getAndSet(newSeqNo.toLong())
                            fetchSuccessful = true
                            postFetchBufferUpdate(changesResponse, shardInactiveWhenBatchAdded)
                        } catch (e: ElasticsearchTimeoutException) {
                            log.info("Timed out waiting for new changes. Current seqNo: $seqNo")
                            timeOutEncountered = true
                            lastFetchTimedOut[i] = timeOutEncountered
                            var numTimeOuts = 0
                            val it = lastFetchTimedOut.elements().asIterator()
                            while(it.hasNext()) {
                                val value = it.next()
                                if (value) {
                                    numTimeOuts++
                                }
                            }
                            if (numTimeOuts == PARALLEL_FETCHES) {
                                log.info("Timeout count = parallelism. Marking index $followerIndexName shard $followerShardId as inactive")
                                markShardInactive = true
                            }
                        } catch (e: NodeNotConnectedException) {
                            log.info("Node not connected. Retrying request using a different node. $e")
                            delay(backOffForNodeDiscovery)
                        } finally {
                            lastFetchTimedOut[i] = timeOutEncountered
                            if (!fetchSuccessful) {
                                postErrorBufferUpdate(markShardInactive, shardInactiveWhenBatchAdded)
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
