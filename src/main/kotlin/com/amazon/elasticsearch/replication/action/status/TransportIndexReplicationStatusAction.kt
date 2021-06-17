package com.amazon.elasticsearch.replication.action.status

import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.routing.ShardsIterator
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.indices.IndicesService
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportActionProxy
import org.elasticsearch.transport.TransportService

class TransportIndexReplicationStatusAction @Inject constructor(threadPool: ThreadPool, clusterService: ClusterService,
                                                                transportService: TransportService, actionFilters: ActionFilters,
                                                                indexNameExpressionResolver: IndexNameExpressionResolver,
                                                                private val indicesService: IndicesService) :
        TransportSingleShardAction<IndexReplicationStatusRequest, StatusResponse>(IndexReplicationStatusAction.NAME,
                threadPool, clusterService, transportService, actionFilters,
                indexNameExpressionResolver, ::IndexReplicationStatusRequest, ThreadPool.Names.SEARCH) {

    init {
        TransportActionProxy.registerProxyAction(transportService, IndexReplicationStatusAction.NAME, ::StatusResponse)
    }

    companion object {
        private val log = LogManager.getLogger(TransportIndexReplicationStatusAction::class.java)
    }

    @Suppress("BlockingMethodInNonBlockingContext")
    override fun asyncShardOperation(request: IndexReplicationStatusRequest?, shardId: ShardId?, listener: ActionListener<StatusResponse>?) {
        val indexName = request?.indexName
        val shards = clusterService.state().routingTable.indicesRouting().get((request?.indexName ?: null)).shards()
        val replayDetailsList: MutableList<ReplayDetails> = mutableListOf<ReplayDetails>()
        val rerstoreDetailsList: MutableList<RestoreDetails> = mutableListOf<RestoreDetails>()
        val ReplicationResponseList: MutableList<ReplicationResponse> = mutableListOf<ReplicationResponse>()
        var state = "restore"
        shards.forEach {
            val shardid = it.value.shardId
            val indexShard = indicesService.indexServiceSafe(shardid.index).getShard(shardid.id)


            var indexState = indexShard.recoveryState().index

            if(indexState.recoveredBytesPercent() >= 100 || indexState.recoveredFilesPercent() >= 100.0) {
                state = "replay"
                rerstoreDetailsList.add(RestoreDetails(indexState.totalBytes(), indexState.recoveredBytes(),
                        indexState.recoveredBytesPercent(), indexState.totalFileCount(), indexState.recoveredFileCount(),
                        indexState.recoveredFilesPercent(), indexState.startTime(), indexState.time(), shardid
                ))
            }


            var seqNo = indexShard.localCheckpoint + 1
            replayDetailsList.add(ReplayDetails(indexShard.lastSyncedGlobalCheckpoint, indexShard.lastSyncedGlobalCheckpoint, seqNo, shardid))
        }

        ReplicationResponseList.add(ReplicationResponse(state,indexName, rerstoreDetailsList, replayDetailsList))
        if (listener != null) {
            listener.onResponse(StatusResponse(ReplicationResponseList))
        }
    }

    override fun shardOperation(request: IndexReplicationStatusRequest?, shardId: ShardId?): StatusResponse {
        throw UnsupportedOperationException("use asyncShardOperation")
    }

    override fun resolveIndex(request: IndexReplicationStatusRequest?): Boolean {
        return true
    }

    override fun getResponseReader(): Writeable.Reader<StatusResponse> {
        return Writeable.Reader { inp: StreamInput -> StatusResponse(inp) }
    }

    override fun shards(state: ClusterState?, request: InternalRequest?): ShardsIterator? {
        return null
    }

}