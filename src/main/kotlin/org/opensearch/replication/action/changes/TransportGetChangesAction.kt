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

package org.opensearch.replication.action.changes

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.opensearch.OpenSearchTimeoutException
import org.opensearch.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.single.shard.TransportSingleShardAction
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.routing.ShardsIterator
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.unit.TimeValue
import org.opensearch.index.shard.ShardId
import org.opensearch.index.translog.Translog
import org.opensearch.indices.IndicesService
import org.opensearch.replication.util.completeWith
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.waitForGlobalCheckpoint
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportActionProxy
import org.opensearch.transport.TransportService
import kotlin.math.min

class TransportGetChangesAction @Inject constructor(threadPool: ThreadPool, clusterService: ClusterService,
                                                    transportService: TransportService, actionFilters: ActionFilters,
                                                    indexNameExpressionResolver: IndexNameExpressionResolver,
                                                    private val indicesService: IndicesService) :
    TransportSingleShardAction<GetChangesRequest, GetChangesResponse>(
        GetChangesAction.NAME, threadPool, clusterService, transportService, actionFilters,
        indexNameExpressionResolver, ::GetChangesRequest, ThreadPool.Names.SEARCH) {

    init {
        TransportActionProxy.registerProxyAction(transportService, GetChangesAction.NAME, ::GetChangesResponse)
    }

    companion object {
        val WAIT_FOR_NEW_OPS_TIMEOUT = TimeValue.timeValueMinutes(1)!!
    }

    override fun shardOperation(request: GetChangesRequest, shardId: ShardId): GetChangesResponse {
        throw UnsupportedOperationException("use asyncShardOperation")
    }

    @Suppress("BlockingMethodInNonBlockingContext")
    override fun asyncShardOperation(request: GetChangesRequest, shardId: ShardId, listener: ActionListener<GetChangesResponse>) {
        GlobalScope.launch(threadPool.coroutineContext(ThreadPool.Names.SEARCH)) {
            // TODO: Figure out if we need to acquire a primary permit here
            listener.completeWith {
                val indexShard = indicesService.indexServiceSafe(shardId.index).getShard(shardId.id)
                if (indexShard.lastSyncedGlobalCheckpoint < request.fromSeqNo) {
                    // There are no new operations to sync. Do a long poll and wait for GlobalCheckpoint to advance. If
                    // the checkpoint doesn't advance by the timeout this throws an ESTimeoutException which the caller
                    // should catch and start a new poll.
                    val gcp = indexShard.waitForGlobalCheckpoint(request.fromSeqNo, WAIT_FOR_NEW_OPS_TIMEOUT)

                    // At this point indexShard.lastKnownGlobalCheckpoint  has advanced but it may not yet have been synced
                    // to the translog, which means we can't return those changes. Return to the caller to retry.
                    // TODO: Figure out a better way to wait for the global checkpoint to be synced to the translog
                    if (indexShard.lastSyncedGlobalCheckpoint < request.fromSeqNo) {
                        assert(gcp > indexShard.lastSyncedGlobalCheckpoint) { "Checkpoint didn't advance at all" }
                        throw OpenSearchTimeoutException("global checkpoint not synced. Retry after a few miliseconds...")
                    }
                }

                // At this point lastSyncedGlobalCheckpoint is at least fromSeqNo
                val toSeqNo = min(indexShard.lastSyncedGlobalCheckpoint, request.toSeqNo)
                indexShard.newChangesSnapshot("odr", request.fromSeqNo, toSeqNo, true).use { snapshot ->
                    val ops = ArrayList<Translog.Operation>(snapshot.totalOperations())
                    var op = snapshot.next()
                    while (op != null) {
                        ops.add(op)
                        op = snapshot.next()
                    }
                    GetChangesResponse(ops, request.fromSeqNo, indexShard.maxSeqNoOfUpdatesOrDeletes)
                }
            }
        }
    }

    override fun resolveIndex(request: GetChangesRequest): Boolean {
        return true
    }

    override fun getResponseReader(): Writeable.Reader<GetChangesResponse> {
        return Writeable.Reader { inp: StreamInput -> GetChangesResponse(inp) }
    }

    override fun shards(state: ClusterState, request: InternalRequest): ShardsIterator {
        // TODO: Investigate using any active shards instead of just primary
        return state.routingTable().shardRoutingTable(request.request().shardId).primaryShardIt()
    }
}