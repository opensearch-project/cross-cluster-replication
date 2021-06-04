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

import org.opensearch.replication.action.changes.GetChangesResponse
import org.opensearch.replication.action.replay.ReplayChangesAction
import org.opensearch.replication.action.replay.ReplayChangesRequest
import org.opensearch.replication.action.replay.ReplayChangesResponse
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.test.runBlockingTest
import org.assertj.core.api.Assertions.assertThat
import org.opensearch.action.ActionListener
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionResponse
import org.opensearch.action.ActionType
import org.opensearch.action.support.replication.ReplicationResponse.ShardInfo
import org.opensearch.index.shard.ShardId
import org.opensearch.index.translog.Translog
import org.opensearch.tasks.TaskId.EMPTY_TASK_ID
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.OpenSearchTestCase.randomList
import org.opensearch.test.client.NoOpClient
import java.util.Locale

@ObsoleteCoroutinesApi
class TranslogSequencerTests : OpenSearchTestCase() {

    class RequestCapturingClient : NoOpClient(TranslogSequencerTests::class.java.simpleName) {
        val requestsReceived = mutableListOf<ReplayChangesRequest>()

        override fun <Req : ActionRequest, Resp : ActionResponse> doExecute(action: ActionType<Resp>,
                                                                            request: Req,
                                                                            listener: ActionListener<Resp>) {
            if (action === ReplayChangesAction.INSTANCE) {
                requestsReceived.add(request as ReplayChangesRequest)
                val resp = ReplayChangesResponse()
                resp.shardInfo = ShardInfo(1, 1)
                @Suppress("UNCHECKED_CAST")
                listener.onResponse(resp as Resp)
            } else {
                super.doExecute(action, request, listener)
            }
        }

        fun reset() {
            requestsReceived.clear()
        }
    }


    val remoteCluster = "remoteCluster"
    val remoteIndex = "remoteIndex"
    val followerShardId = ShardId("follower", "follower_uuid", 0)
    val client = RequestCapturingClient()
    init {
        closeAfterSuite(client)
    }

    override fun tearDown() {
        client.reset()
        super.tearDown()
    }

    @ExperimentalCoroutinesApi
    fun `test sequencer out of order`() = runBlockingTest {
        val startSeqNo = randomNonNegativeLong()
        val rateLimiter = Semaphore(10)
        val sequencer = TranslogSequencer(this, followerShardId, remoteCluster, remoteIndex, EMPTY_TASK_ID,
                                          client, rateLimiter, startSeqNo)

        // Send requests out of order (shuffled seqNo) and await for them to be processed.
        var batchSeqNo = startSeqNo
        val batches = randomList(1, rateLimiter.availablePermits) {
            val (batch, lastSeqNo) = randomChangesResponse(batchSeqNo)
            batchSeqNo = lastSeqNo
            batch
        }
        batches.shuffled().forEach {
            rateLimiter.acquire()
            sequencer.send(it)
        }
        sequencer.close()

        // Now verify that there was one replay request for every batch of changes that was sent
        assertThat(client.requestsReceived.size).isEqualTo(batches.size)
        batches.zip(client.requestsReceived).forEach {  (batch, req) ->
            assertThat(batch.changes.first().seqNo()).isEqualTo(req.changes.first().seqNo())
        }
    }

    fun randomChangesResponse(startSeqNo: Long) : Pair<GetChangesResponse, Long> {
        var seqNo = startSeqNo
        val changes = randomList(1, randomIntBetween(1, 512)) {
            seqNo = seqNo.inc()
            Translog.Index("_doc", randomAlphaOfLength(10).toLowerCase(Locale.ROOT), seqNo,
                           1L, "{}".toByteArray(Charsets.UTF_8))
        }
        return Pair(GetChangesResponse(changes, startSeqNo.inc(), startSeqNo), seqNo)
    }
}