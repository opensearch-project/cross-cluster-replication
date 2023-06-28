/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.replication.task.shard

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.test.runBlockingTest
import org.assertj.core.api.Assertions.assertThat
import org.mockito.Mockito
import org.opensearch.action.ActionListener
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionResponse
import org.opensearch.action.ActionType
import org.opensearch.action.support.replication.ReplicationResponse.ShardInfo
import org.opensearch.common.settings.Settings
import org.opensearch.index.IndexService
import org.opensearch.index.shard.IndexShard
import org.opensearch.index.shard.ShardId
import org.opensearch.index.translog.Translog
import org.opensearch.indices.IndicesService
import org.opensearch.replication.action.changes.GetChangesResponse
import org.opensearch.replication.action.replay.ReplayChangesAction
import org.opensearch.replication.action.replay.ReplayChangesRequest
import org.opensearch.replication.action.replay.ReplayChangesResponse
import org.opensearch.replication.metadata.ReplicationOverallState
import org.opensearch.replication.metadata.store.ReplicationContext
import org.opensearch.replication.metadata.store.ReplicationMetadata
import org.opensearch.replication.metadata.store.ReplicationStoreMetadataType
import org.opensearch.replication.util.indicesService
import org.opensearch.tasks.TaskId.EMPTY_TASK_ID
import org.opensearch.test.OpenSearchTestCase
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


    val leaderAlias = "leaderAlias"
    val leaderIndex = "leaderIndex"
    val followerShardId = ShardId("follower", "follower_uuid", 0)
    val replicationMetadata = ReplicationMetadata(leaderAlias, ReplicationStoreMetadataType.INDEX.name, ReplicationOverallState.RUNNING.name, "test user",
            ReplicationContext(followerShardId.indexName, null), ReplicationContext(leaderIndex, null), Settings.EMPTY)
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
        val stats = FollowerClusterStats()
        stats.stats[followerShardId]  = FollowerShardMetric()
        val startSeqNo = randomNonNegativeLong()
        indicesService = Mockito.mock(IndicesService::class.java)
        val followerIndexService = Mockito.mock(IndexService::class.java)
        val indexShard = Mockito.mock(IndexShard::class.java)
        Mockito.`when`(indicesService.indexServiceSafe(followerShardId.index)).thenReturn(followerIndexService)
        Mockito.`when`(followerIndexService.getShard(followerShardId.id)).thenReturn(indexShard)
        val sequencer = TranslogSequencer(this, replicationMetadata, followerShardId, leaderAlias, leaderIndex, EMPTY_TASK_ID,
                                          client, startSeqNo, stats, 2)

        // Send requests out of order (shuffled seqNo) and await for them to be processed.
        var batchSeqNo = startSeqNo
        val batches = randomList(1, 5) {
            val (batch, lastSeqNo) = randomChangesResponse(batchSeqNo)
            batchSeqNo = lastSeqNo
            batch
        }
        batches.shuffled().forEach {
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
        return Pair(GetChangesResponse(changes, startSeqNo.inc(), startSeqNo, -1), seqNo)
    }
}