package com.amazon.elasticsearch.replication.action.status

import com.amazon.elasticsearch.replication.metadata.ReplicationMetadataManager
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.DefaultShardOperationFailedException
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.routing.*
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.index.IndexService
import org.elasticsearch.indices.IndicesService
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService
import java.io.IOException
import java.util.*

class TranportShardsInfoAction  @Inject constructor(clusterService: ClusterService,
                                                    transportService: TransportService,
                                                    threadPool: ThreadPool,
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
            return ShardInfoResponse(shardRouting.shardId(),"BOOTSTRAPPING",
                    RestoreDetails(indexState.totalBytes(), indexState.recoveredBytes(),
                            indexState.recoveredBytesPercent(), indexState.totalFileCount(), indexState.recoveredFileCount(),
                            indexState.recoveredFilesPercent(), indexState.startTime(), indexState.time()))
        }
        var seqNo = indexShard.localCheckpoint + 1
        return ShardInfoResponse(shardRouting.shardId(),"SYNCING", ReplayDetails(indexShard.lastKnownGlobalCheckpoint,
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
