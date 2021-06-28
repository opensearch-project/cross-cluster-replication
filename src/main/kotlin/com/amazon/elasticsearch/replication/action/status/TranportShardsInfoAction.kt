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
import org.elasticsearch.cluster.routing.PlainShardsIterator
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
import java.util.*

class TranportShardsInfoAction  @Inject constructor(clusterService: ClusterService,
                                                    transportService: TransportService,
                                                    replicationMetadataManager: ReplicationMetadataManager,
                                                    threadPool: ThreadPool,
                                                    actionFilters: ActionFilters,
                                                    client: Client,
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
        val indexShard: IndexShard = indicesService.indexServiceSafe(shardRouting.shardId().index).getShard(shardRouting.shardId().id())
        var indexState = indexShard.recoveryState().index
        var seqNo = indexShard.localCheckpoint + 1
        if (indexShard.recoveryState().recoverySource.type.equals(org.elasticsearch.cluster.routing.RecoverySource.Type.SNAPSHOT) and
                (indexState.recoveredBytesPercent() <100)) {
            return ShardInfoResponse(shardRouting.shardId(),"BOOTSTRAPPING",
                    RestoreDetails(indexState.totalBytes(), indexState.recoveredBytes(),
                            indexState.recoveredBytesPercent(), indexState.totalFileCount(), indexState.recoveredFileCount(),
                            indexState.recoveredFilesPercent(), indexState.startTime(), indexState.time()))
        }
        return ShardInfoResponse(shardRouting.shardId(),"SYNCING", ReplayDetails(indexShard.lastKnownGlobalCheckpoint,
                indexShard.lastSyncedGlobalCheckpoint, seqNo))
    }

    override fun shards(clusterState: ClusterState, request: ShardInfoRequest?, concreteIndices: Array<String?>?): ShardsIterator? {
        val activePrimaryShardsGrouped = clusterState.routingTable().activePrimaryShardsGrouped(concreteIndices, false)
        val shards: MutableList<ShardRouting> = ArrayList()
        activePrimaryShardsGrouped.forEach {
            shards.addAll(it.shardRoutings)
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
