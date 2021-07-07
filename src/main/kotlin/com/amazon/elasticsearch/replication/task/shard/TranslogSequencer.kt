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
import com.amazon.elasticsearch.replication.TranslogBuffer
import com.amazon.elasticsearch.replication.action.changes.GetChangesResponse
import com.amazon.elasticsearch.replication.action.replay.ReplayChangesAction
import com.amazon.elasticsearch.replication.action.replay.ReplayChangesRequest
import com.amazon.elasticsearch.replication.action.replay.ReplayChangesResponse
import com.amazon.elasticsearch.replication.metadata.store.ReplicationMetadata
import com.amazon.elasticsearch.replication.util.suspendExecute
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import org.elasticsearch.client.Client
import org.elasticsearch.common.lease.Releasable
import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.index.translog.Translog
import org.elasticsearch.tasks.TaskId
import java.io.Closeable
import java.util.concurrent.ConcurrentHashMap

/**
 * A TranslogSequencer allows multiple producers of [Translog.Operation]s to write them in sequence number order to an
 * index.  It internally uses an [actor] to serialize writes to the index. Producer can call the [send] method
 * to add a batch of operations to the queue.  If the queue is full the producer will be suspended.  Operations can be
 * sent out of order i.e. the operation with sequence number 2 can be sent before the operation with sequence number 1.
 * In this case the Sequencer will internally buffer the operations that cannot be delivered until the missing in-order
 * operations arrive.
 *
 * This uses the ObsoleteCoroutinesApi actor API.  As described in the [actor] docs there is no current replacement for
 * this API and a new one is being worked on to which we can migrate when needed.
 */
@ObsoleteCoroutinesApi
class TranslogSequencer(scope: CoroutineScope, private val replicationMetadata: ReplicationMetadata,
                        private val followerShardId: ShardId,
                        private val remoteCluster: String, private val remoteIndexName: String,
                        private val parentTaskId: TaskId, private val client: Client,
                        initialSeqNo: Long, private val translogBuffer: TranslogBuffer) {

    private val unAppliedChanges = ConcurrentHashMap<Long, Pair<GetChangesResponse, Releasable> >()
    private val log = Loggers.getLogger(javaClass, followerShardId)!!
    private val completed = CompletableDeferred<Unit>()

    // Channel is unlimited capacity as changes can arrive out of order but must be applied in-order.  If the channel
    // had limited capacity it could deadlock.
    private val sequencer = scope.actor<Unit>(capacity = Channel.UNLIMITED) {
        // Exceptions thrown here will mark the channel as failed and the next attempt to send to the channel will
        // raise the same exception.  See [SendChannel.close] method for details.
        var highWatermark = initialSeqNo
        for (m in channel) {
            launch {
                translogBuffer.acquireRateLimiter2()
                try {
                    while (unAppliedChanges.containsKey(highWatermark + 1)) {
                        val next = unAppliedChanges.remove(highWatermark + 1)!!
                        val replayRequest = ReplayChangesRequest(followerShardId, next.first.changes, next.first.maxSeqNoOfUpdatesOrDeletes,
                                remoteCluster, remoteIndexName)
                        replayRequest.parentTask = parentTaskId
                        val replayResponse : ReplayChangesResponse
                        try {
                            replayResponse = client.suspendExecute(replicationMetadata, ReplayChangesAction.INSTANCE, replayRequest)
                        } finally {
                            next.second.close()
                        }
                        if (replayResponse.shardInfo.failed > 0) {
                            replayResponse.shardInfo.failures.forEachIndexed { i, failure ->
                                log.error("Failed replaying changes. Failure:$i:$failure")
                            }
                            throw ReplicationException("failed to replay changes", replayResponse.shardInfo.failures)
                        }
                        highWatermark = next.first.changes.lastOrNull()?.seqNo() ?: highWatermark
                    }
                } finally {
                    translogBuffer.releaseRateLimiter2()
                }
            }
        }
        completed.complete(Unit)
    }

    suspend fun close() {
        sequencer.close()
        completed.await()
    }

    suspend fun send(changes : GetChangesResponse, closeable: Releasable) {
        if (unAppliedChanges.containsKey(changes.fromSeqNo)) {
            val existingBatch = unAppliedChanges[changes.fromSeqNo]!!
            if (existingBatch.first.maxSeqNoOfUpdatesOrDeletes > changes.maxSeqNoOfUpdatesOrDeletes) {
                // discard current batch as an already existing batch is already present which contains all the changes
                // in the incoming batch
                closeable.close()
            } else {
                // there's a batch existing which has same starting seq no but has changes till less seq no, so we're
                // overwriting that batch, so call its closable
                existingBatch.second.close()
            }
        }
        unAppliedChanges[changes.fromSeqNo] = Pair(changes, closeable)
        sequencer.send(Unit)
    }
}
