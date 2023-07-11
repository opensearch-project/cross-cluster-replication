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

package org.opensearch.replication.action.status

import org.apache.logging.log4j.LogManager
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.DefaultShardOperationFailedException
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.routing.*
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.index.IndexService
import org.opensearch.indices.IndicesService
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import java.io.IOException

class TranportShardsInfoAction  @Inject constructor(clusterService: ClusterService,
                                                    transportService: TransportService,
                                                    actionFilters: ActionFilters,
                                                    indexNameExpressionResolver: IndexNameExpressionResolver?,
                                                    private val indicesService: IndicesService
)
       : TransportBroadcastByNodeAction<
                ShardInfoRequest,
                ReplicationStatusResponse,
                ShardInfoResponse> ( ShardsInfoAction.NAME,
        clusterService,transportService,actionFilters,indexNameExpressionResolver,Writeable.Reader { X -> ShardInfoRequest(X) },
        ThreadPool.Names.MANAGEMENT
        ) {


    companion object {
        private val log = LogManager.getLogger(TranportShardsInfoAction::class.java)
    }

    @Throws(IOException::class)
    override fun readShardResult(si: StreamInput): ShardInfoResponse? {
        return ShardInfoResponse(si)
    }

    override fun newResponse(
            request: ShardInfoRequest,
            totalShards: Int,
            successfulShards: Int,
            failedShards: Int,
            shardInfoRespons: List<ShardInfoResponse>,
            shardFailures: List<DefaultShardOperationFailedException>,
            clusterState: ClusterState
    ): ReplicationStatusResponse {
        return (ReplicationStatusResponse(totalShards, successfulShards, failedShards, shardFailures, shardInfoRespons))
    }

    @Throws(IOException::class)
    override fun readRequestFrom(si: StreamInput): ShardInfoRequest {
        return ShardInfoRequest(si)
    }

    @Throws(IOException::class)
    override fun shardOperation(request: ShardInfoRequest, shardRouting: ShardRouting): ShardInfoResponse {
        val indexService: IndexService = indicesService.indexServiceSafe(shardRouting.shardId().index)
        val indexShard = indexService.getShard(shardRouting.shardId().id())

        var indexState = indexShard.recoveryState().index
        if (indexShard.recoveryState().recoverySource.type.equals(RecoverySource.Type.SNAPSHOT) and
                (indexState.recoveredBytesPercent() <100)) {
            return ShardInfoResponse(shardRouting.shardId(),ShardInfoResponse.BOOTSTRAPPING,
                    RestoreDetails(indexState.totalBytes(), indexState.recoveredBytes(),
                            indexState.recoveredBytesPercent(), indexState.totalFileCount(), indexState.recoveredFileCount(),
                            indexState.recoveredFilesPercent(), indexState.startTime(), indexState.time()))
        }
        var seqNo = indexShard.localCheckpoint + 1
        return ShardInfoResponse(shardRouting.shardId(),ShardInfoResponse.SYNCING, ReplayDetails(indexShard.lastKnownGlobalCheckpoint,
                indexShard.lastSyncedGlobalCheckpoint, seqNo))
    }

    override fun shards(clusterState: ClusterState, request: ShardInfoRequest?, concreteIndices: Array<String?>?): ShardsIterator? {
        var shardRoutingList = clusterState.routingTable().allShards(request!!.indexName)
        val shards: MutableList<ShardRouting> = ArrayList()
        shardRoutingList.forEach {
            if(it.primary()) {
                shards.add(it)
            }
        }
        return PlainShardsIterator(shards)
    }

    override fun checkRequestBlock(state: ClusterState, request: ShardInfoRequest?, concreteIndices: Array<String?>?):
            ClusterBlockException? {
        return null
    }

    override fun checkGlobalBlock(state: ClusterState?, request: ShardInfoRequest?): ClusterBlockException? {
        return null
    }
}
