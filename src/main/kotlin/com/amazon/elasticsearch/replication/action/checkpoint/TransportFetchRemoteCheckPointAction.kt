package com.amazon.elasticsearch.replication.action.checkpoint



import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.DefaultShardOperationFailedException
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.routing.ShardRouting
import org.elasticsearch.cluster.routing.ShardsIterator
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.index.shard.IndexShard
import org.elasticsearch.indices.IndicesService
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService
import java.io.IOException

class TransportFetchRemoteCheckPointAction :
        TransportBroadcastByNodeAction<
                FetchGlobalCheckPointRequest,
                RemoteGlobalCheckPointResponse,
                RemoteCheckPointShardResponse> {

    private val log = LogManager.getLogger(javaClass)

    @Inject
    constructor(
            clusterService: ClusterService,
            transportService: TransportService,
            indicesService: IndicesService,
            actionFilters: ActionFilters,
            indexNameExpressionResolver: IndexNameExpressionResolver?
    ) : super(
            FetchGlobalCheckPointAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            Writeable.Reader { X ->  FetchGlobalCheckPointRequest(X) },
            ThreadPool.Names.MANAGEMENT
    ) {
        this.indicesService = indicesService
    }

    private val indicesService: IndicesService

    @Throws(IOException::class)
    override fun readShardResult(si: StreamInput): RemoteCheckPointShardResponse? {
        return RemoteCheckPointShardResponse(si)
    }

    override fun newResponse(
            request: FetchGlobalCheckPointRequest,
            totalShards: Int,
            successfulShards: Int,
            failedShards: Int,
            shardResponses: List<RemoteCheckPointShardResponse>,
            shardFailures: List<DefaultShardOperationFailedException>,
            clusterState: ClusterState
    ): RemoteGlobalCheckPointResponse {
        return RemoteGlobalCheckPointResponse(totalShards, successfulShards, failedShards, shardFailures, shardResponses)
    }

    @Throws(IOException::class)
    override fun readRequestFrom(si: StreamInput): FetchGlobalCheckPointRequest {
        return FetchGlobalCheckPointRequest(si)
    }

    @Throws(IOException::class)
    override fun shardOperation(request: FetchGlobalCheckPointRequest, shardRouting: ShardRouting): RemoteCheckPointShardResponse {
        val indexShard: IndexShard = indicesService.indexServiceSafe(shardRouting.shardId().index).getShard(shardRouting.shardId().id())
        return RemoteCheckPointShardResponse(shardRouting.shardId(), indexShard.lastSyncedGlobalCheckpoint)
    }

    override fun shards(clusterState: ClusterState, request: FetchGlobalCheckPointRequest?, concreteIndices: Array<String?>?): ShardsIterator? {
        return clusterState.routingTable().allShards(concreteIndices)
    }

    override fun checkGlobalBlock(state: ClusterState, request: FetchGlobalCheckPointRequest?): ClusterBlockException? {
        return null
    }

    override fun checkRequestBlock(state: ClusterState, request: FetchGlobalCheckPointRequest?, concreteIndices: Array<String?>?):
            ClusterBlockException? {
        return null
    }
}