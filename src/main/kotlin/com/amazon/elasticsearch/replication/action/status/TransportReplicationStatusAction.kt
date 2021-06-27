package com.amazon.elasticsearch.replication.action.status


import com.amazon.elasticsearch.replication.metadata.ReplicationMetadataManager
import com.amazon.elasticsearch.replication.util.coroutineContext
import kotlinx.coroutines.*
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.routing.ShardsIterator
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.indices.IndicesService
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService
import org.elasticsearch.action.support.DefaultShardOperationFailedException
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.routing.ShardRouting
import org.elasticsearch.index.analysis.AnalysisRegistry
import org.elasticsearch.index.shard.IndexShard
import java.io.IOException


class TransportReplicationStatusAction :
        TransportBroadcastByNodeAction<
                ReplicationStatusRequest,
                ReplicationStatusResponse,
                ReplicationStatusShardResponse> {

    private val log = LogManager.getLogger(javaClass)

    @Inject
    constructor(
            clusterService: ClusterService,
            transportService: TransportService,
            indicesService: IndicesService,
            replicationMetadataManager: ReplicationMetadataManager,
            threadPool: ThreadPool,
            actionFilters: ActionFilters,
            analysisRegistry: AnalysisRegistry,
            indexNameExpressionResolver: IndexNameExpressionResolver?
    ) : super(
            ReplicationStatusAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            Writeable.Reader { ReplicationStatusRequest() },
            ThreadPool.Names.MANAGEMENT
    ) {
        this.replicationMetadataManager = replicationMetadataManager
        this.analysisRegistry = analysisRegistry
        this.indicesService = indicesService
        this.threadPool = threadPool
    }

    private val indicesService: IndicesService
    private val analysisRegistry: AnalysisRegistry
    private val replicationMetadataManager: ReplicationMetadataManager
    private val threadPool: ThreadPool

    @Throws(IOException::class)
    override fun readShardResult(si: StreamInput): ReplicationStatusShardResponse? {
        return ReplicationStatusShardResponse(si)
    }

    override fun newResponse(
            request: ReplicationStatusRequest,
            totalShards: Int,
            successfulShards: Int,
            failedShards: Int,
            shardResponses: List<ReplicationStatusShardResponse>,
            shardFailures: List<DefaultShardOperationFailedException>,
            clusterState: ClusterState
    ): ReplicationStatusResponse {
        var state = getReplicationState(request, shardResponses)
        return ReplicationStatusResponse(totalShards, successfulShards, failedShards, shardFailures, shardResponses, state)
    }

    private fun getReplicationState(request: ReplicationStatusRequest, shardResponses: List<ReplicationStatusShardResponse>): String {
        var state = "STOPPED"
        runBlocking {
            withTimeout(1000) {
                val job = CoroutineScope(threadPool.coroutineContext()).launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
                    val metadata = replicationMetadataManager.getIndexReplicationMetadata(request!!.indices()[0])
                    state = if (metadata.overallState.isNullOrEmpty()) "STOPPED" else metadata.overallState
                }
                job.join()
            }
        }
        if (shardResponses.size > 0) {
                var restoredetails = shardResponses.get(0).restoreDetails
                if ((restoredetails.recovereyPercentage < 100 || restoredetails.fileRecovereyPercentage < 100)) {
                    state = if (state == "RUNNING") "RESTORE" else state
                } else {
                    state = if (state == "RUNNING") "REPLAY" else state
                }

        }
        return state
    }

    @Throws(IOException::class)
    override fun readRequestFrom(si: StreamInput): ReplicationStatusRequest {
        return ReplicationStatusRequest(si)
    }

    @Throws(IOException::class)
    override fun shardOperation(request: ReplicationStatusRequest, shardRouting: ShardRouting): ReplicationStatusShardResponse {
        val indexShard: IndexShard = indicesService.indexServiceSafe(shardRouting.shardId().index).getShard(shardRouting.shardId().id())
        var indexState = indexShard.recoveryState().index
        var seqNo = indexShard.localCheckpoint + 1
        return ReplicationStatusShardResponse(shardRouting.shardId(), ReplayDetails(indexShard.lastSyncedGlobalCheckpoint, indexShard.lastKnownGlobalCheckpoint, seqNo),
                RestoreDetails(indexState.totalBytes(), indexState.recoveredBytes(),
                        indexState.recoveredBytesPercent(), indexState.totalFileCount(), indexState.recoveredFileCount(),
                        indexState.recoveredFilesPercent(), indexState.startTime(), indexState.time()))
    }


    override fun shards(clusterState: ClusterState, request: ReplicationStatusRequest?, concreteIndices: Array<String?>?): ShardsIterator? {
        return clusterState.routingTable().allShards(concreteIndices)
    }


    override fun checkRequestBlock(state: ClusterState, request: ReplicationStatusRequest?, concreteIndices: Array<String?>?):
            ClusterBlockException? {
        return null
    }

    override fun checkGlobalBlock(state: ClusterState?, request: ReplicationStatusRequest?): ClusterBlockException? {
        return null
    }
}