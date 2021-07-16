package com.amazon.elasticsearch.replication.action.status


import com.amazon.elasticsearch.replication.ReplicationException
import com.amazon.elasticsearch.replication.metadata.ReplicationMetadataManager
import com.amazon.elasticsearch.replication.util.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ResourceNotFoundException
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
                    var reason = metadata.reason
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
                    followerResponse.reason = reason
                    populateAggregatedResponse(followerResponse)
                    if (!request.verbose) {
                        followerResponse.isVerbose = false
                    }
                    followerResponse
                } catch (e : ResourceNotFoundException) {
                    log.error("got ResourceNotFoundException while querying for status ",e)
                    ReplicationStatusResponse("REPLICATION NOT IN PROGRESS")
                } catch(e : Exception) {
                    log.error("got Exception while querying for status ",e)
                    throw ReplicationException("failed to fetch replication status")
                }
            }
        }
    }

    private fun populateAggregatedResponse(followerResponse: ReplicationStatusResponse) {
        var aggregatedRemoteCheckpoint: Long = 0
        var aggregatedLocalCheckpoint: Long = 0
        var aggregatedSeqNo: Long = 0
        var anyShardInReplay: Boolean = false
        var anyShardInRestore: Boolean = false
        var aggregateTotalBytes: Long = 0
        var aggregateRecoveredBytes: Long = 0
        var aggregateRecovereyPercentage: Float = 0F
        var aggregateTotalFiles: Int = 0
        var aggregateRecoveredFiles: Int = 0
        var aggregateFileRecovereyPercentage: Float = 0F
        var startTime: Long = Long.MAX_VALUE
        var time: Long = 0
        var numberOfShardsiInRestore: Int = 0


        followerResponse.shardInfoResponse.forEach {
            if (it.isReplayDetailsInitialized()) {
                aggregatedRemoteCheckpoint += it.replayDetails.remoteCheckpoint()
                aggregatedLocalCheckpoint += it.replayDetails.localCheckpoint()
                aggregatedSeqNo += it.replayDetails.seqNo()
                anyShardInReplay = true
            }
            if (it.isRestoreDetailsInitialized()) {
                anyShardInRestore = true
                aggregateTotalBytes += it.restoreDetails.totalBytes
                aggregateRecoveredBytes += it.restoreDetails.recoveredBytes
                aggregateRecovereyPercentage = (numberOfShardsiInRestore * aggregateRecovereyPercentage + it.restoreDetails.recovereyPercentage) / (numberOfShardsiInRestore + 1)
                aggregateFileRecovereyPercentage = (numberOfShardsiInRestore * aggregateFileRecovereyPercentage + it.restoreDetails.fileRecovereyPercentage) / (numberOfShardsiInRestore + 1)
                numberOfShardsiInRestore++
                aggregateTotalFiles += it.restoreDetails.totalFiles
                aggregateRecoveredFiles += it.restoreDetails.recoveredFiles
                startTime = Math.min(startTime, it.restoreDetails.startTime)
                time = Math.max(time, it.restoreDetails.time)
            }
        }
        if (anyShardInReplay) {
            followerResponse.aggregatedReplayDetails = ReplayDetails(aggregatedRemoteCheckpoint, aggregatedLocalCheckpoint, aggregatedSeqNo)
        }
        if (anyShardInRestore) {
            followerResponse.aggregatedRestoreDetails = RestoreDetails(aggregateTotalBytes, aggregateRecoveredBytes, aggregateRecovereyPercentage
                    , aggregateTotalFiles, aggregateRecoveredFiles, aggregateFileRecovereyPercentage, startTime, time)
        }
    }
}

