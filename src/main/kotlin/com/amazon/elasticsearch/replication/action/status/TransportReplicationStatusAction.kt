package com.amazon.elasticsearch.replication.action.status


import com.amazon.elasticsearch.replication.action.checkpoint.FetchGlobalCheckPointAction
import com.amazon.elasticsearch.replication.action.checkpoint.FetchGlobalCheckPointRequest
import com.amazon.elasticsearch.replication.metadata.ReplicationMetadataManager
import com.amazon.elasticsearch.replication.util.coroutineContext
import com.amazon.elasticsearch.replication.util.suspendExecute
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
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.routing.ShardRouting
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
            client: Client,
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
        this.indicesService = indicesService
        this.threadPool = threadPool
        this.client = client
    }

    private val indicesService: IndicesService
    private val replicationMetadataManager: ReplicationMetadataManager
    private val threadPool: ThreadPool
    private val client: Client

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
        return updateReplicationStatusResponseWithRemoteCheckPoints(ReplicationStatusResponse(totalShards, successfulShards, failedShards, shardFailures, shardResponses),request)
    }

    private fun updateReplicationStatusResponseWithRemoteCheckPoints(replicationStatusResponse: ReplicationStatusResponse, request: ReplicationStatusRequest): ReplicationStatusResponse {
        var shardResponses = replicationStatusResponse.replicationShardResponse
        var status = "STOPPED"
        runBlocking {
            withTimeout(1000) {
                val job = CoroutineScope(threadPool.coroutineContext()).launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
                    val metadata = replicationMetadataManager.getIndexReplicationMetadata(request!!.indices()[0])
                    val remoteClient = client.getRemoteClusterClient(metadata.connectionName)
                    val response = remoteClient.suspendExecute(FetchGlobalCheckPointAction.INSTANCE,
                            FetchGlobalCheckPointRequest(metadata.leaderContext.resource))
                    status = if (metadata.overallState.isNullOrEmpty()) "STOPPED" else metadata.overallState
                    response.shardResponses.listIterator().forEach {
                        val leaderShardName = it.shardId.toString()
                        val remoteCheckPoint = it.shardGlobalCheckPoint
                        shardResponses.listIterator().forEach {
                            if(leaderShardName.equals(it.shardId.toString().replace(metadata.followerContext.resource, metadata.leaderContext.resource))) {
                                it.replayDetails.remoteCheckpoint = remoteCheckPoint
                            }
                        }
                        replicationStatusResponse.replicationShardResponse = shardResponses
                    }
                }
                job.join()
            }
        }

        if (replicationStatusResponse.replicationShardResponse.size > 0) {
            var restoredetails = replicationStatusResponse.replicationShardResponse.get(0).restoreDetails
            if ((restoredetails.recovereyPercentage < 100 || restoredetails.fileRecovereyPercentage < 100)) {
                status = if (status == "RUNNING") "RESTORE" else status
            } else {
                status = if (status == "RUNNING") "REPLAY" else status
            }
        }
        replicationStatusResponse.status = status
        return replicationStatusResponse;
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
        return ReplicationStatusShardResponse(shardRouting.shardId(), ReplayDetails(indexShard.lastSyncedGlobalCheckpoint, seqNo),
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