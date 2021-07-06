package com.amazon.elasticsearch.replication.action.status


import com.amazon.elasticsearch.replication.metadata.ReplicationMetadataManager
import com.amazon.elasticsearch.replication.util.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.client.Client
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.tasks.Task
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService

class TransportReplicationStatusAction @Inject constructor(transportService: TransportService,
                                                           val threadPool: ThreadPool,
                                                           actionFilters: ActionFilters,
                                                           private val client : Client,
                                                           private val replicationMetadataManager: ReplicationMetadataManager) :
        HandledTransportAction<ShardInfoRequest, ReplicationStatusResponse>(ReplicationStatusAction.NAME,
                transportService, actionFilters, ::ShardInfoRequest),
        CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportReplicationStatusAction::class.java)
    }


    override fun doExecute(task: Task, request: ShardInfoRequest, listener: ActionListener<ReplicationStatusResponse>) {
        launch(threadPool.coroutineContext()) {
            listener.completeWith {
                try {
                    val metadata = replicationMetadataManager.getIndexReplicationMetadata(request!!.indices()[0])
                    val remoteClient = client.getRemoteClusterClient(metadata.connectionName)
                    var status = if (metadata.overallState.isNullOrEmpty()) "STOPPED" else metadata.overallState
                    var followerResponse = client.suspendExecute(ShardsInfoAction.INSTANCE,
                            ShardInfoRequest(metadata.followerContext.resource),true)
                    var leaderResponse = remoteClient.suspendExecute(ShardsInfoAction.INSTANCE,
                            ShardInfoRequest(metadata.leaderContext.resource),true)

                    if (followerResponse.shardInfoResponse.size > 0) {
                        status =  if (status == "RUNNING") followerResponse.shardInfoResponse.get(0).status else status
                    }
                    if (!status.equals("BOOTSTRAPPING")) {
                        var shardResponses = followerResponse.shardInfoResponse
                        leaderResponse.shardInfoResponse.listIterator().forEach {
                            val leaderShardName = it.shardId.toString()
                            if (it.isReplayDetailsInitialized()) {
                                val remoteCheckPoint = it.replayDetails.remoteCheckpoint
                                shardResponses.listIterator().forEach {
                                    if (it.isReplayDetailsInitialized()) {
                                        if (leaderShardName.equals(it.shardId.toString()
                                                        .replace(metadata.followerContext.resource, metadata.leaderContext.resource))) {
                                            it.replayDetails.remoteCheckpoint = remoteCheckPoint
                                        }
                                    }
                                }
                                followerResponse.shardInfoResponse = shardResponses
                            }
                        }
                    }
                    followerResponse.connectionAlias = metadata.connectionName
                    followerResponse.followerIndexName = metadata.followerContext.resource
                    followerResponse.leaderIndexName = metadata.leaderContext.resource
                    followerResponse.status = status
                    followerResponse
                } catch(e : Exception) {
                    // TODO : when we get resoucenotfound exception show replication status
                    //  as not in progess and in case of generic excpetion, throw replication_exception.
                    ReplicationStatusResponse("REPLICATION NOT IN PROGRESS")
                }
            }
        }
    }
}

