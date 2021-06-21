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

package com.amazon.elasticsearch.replication.action.resume

import com.amazon.elasticsearch.replication.action.index.ReplicateIndexResponse
import com.amazon.elasticsearch.replication.action.replicationstatedetails.UpdateReplicationStateDetailsRequest
import com.amazon.elasticsearch.replication.metadata.*
import com.amazon.elasticsearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import com.amazon.elasticsearch.replication.task.ReplicationState
import com.amazon.elasticsearch.replication.task.index.IndexReplicationExecutor
import com.amazon.elasticsearch.replication.task.index.IndexReplicationParams
import com.amazon.elasticsearch.replication.task.index.IndexReplicationState
import com.amazon.elasticsearch.replication.util.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.ResourceAlreadyExistsException
import org.elasticsearch.ResourceNotFoundException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.master.AcknowledgedRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.support.master.TransportMasterNodeAction
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.AckedClusterStateUpdateTask
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.ClusterStateTaskExecutor
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.block.ClusterBlockLevel
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.metadata.Metadata
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService
import java.io.IOException

class TransportResumeIndexReplicationAction @Inject constructor(transportService: TransportService,
                                                                clusterService: ClusterService,
                                                                threadPool: ThreadPool,
                                                                actionFilters: ActionFilters,
                                                                indexNameExpressionResolver:
                                                              IndexNameExpressionResolver,
                                                                val client: Client) :
    TransportMasterNodeAction<ResumeIndexReplicationRequest, AcknowledgedResponse> (ResumeIndexReplicationAction.NAME,
            transportService, clusterService, threadPool, actionFilters, ::ResumeIndexReplicationRequest,
            indexNameExpressionResolver), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportResumeIndexReplicationAction::class.java)
    }

    override fun checkBlock(request: ResumeIndexReplicationRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    @Throws(Exception::class)
    override fun masterOperation(request: ResumeIndexReplicationRequest, state: ClusterState,
                                 listener: ActionListener<AcknowledgedResponse>) {
        launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            listener.completeWith {
                log.info("Resuming index replication on index:" + request.indexName)
                validateResumeReplicationRequest(request)

                val stateUpdateResponse : AcknowledgedResponse =
                    clusterService.waitForClusterStateUpdate("Resume_replication") { l -> ResumeReplicationTask(request, l)}
                if (!stateUpdateResponse.isAcknowledged) {
                    throw ElasticsearchException("Failed to update cluster state")
                }

                val currentReplicationMetadata = state.metadata().custom(ReplicationMetadata.NAME)
                        ?: ReplicationMetadata.EMPTY

                val clusterAlias = currentReplicationMetadata.replicatedIndices.entries.firstOrNull {
                    it.value.containsKey(request.indexName)
                }?.key

                val leaderIndex = currentReplicationMetadata.replicatedIndices.get(clusterAlias)?.get(request.indexName)

                if (leaderIndex == null || clusterAlias == null) {
                    throw IllegalStateException("Unknown value of leader index or cluster alias")
                }

                val remoteMetadata = getRemoteIndexMetadata(clusterAlias, leaderIndex)

                val params = IndexReplicationParams(clusterAlias, remoteMetadata.index, request.indexName)

                if (!isResumable(params)) {
                    throw ResourceNotFoundException("Retention lease doesn't exist. Replication can't be resumed for ${request.indexName}")
                }

                updateReplicationStateToStarted(request.indexName)

                val task = persistentTasksService.startTask("replication:index:${request.indexName}",
                        IndexReplicationExecutor.TASK_NAME, params)

                if (!task.isAssigned) {
                    log.error("Failed to assign task")
                    listener.onResponse(ReplicateIndexResponse(false))
                }

                // Now wait for the replication to start and the follower index to get created before returning
                persistentTasksService.waitForTaskCondition(task.id, request.timeout()) { t ->
                    val replicationState = (t.state as IndexReplicationState?)?.state
                    replicationState == ReplicationState.FOLLOWING
                }

                AcknowledgedResponse(true)
            }
        }
    }

    private suspend fun isResumable(params :IndexReplicationParams): Boolean {
        var isResumable = true
        val remoteClient = client.getRemoteClusterClient(params.remoteCluster)
        val shards = clusterService.state().routingTable.indicesRouting().get(params.followerIndexName).shards()
        val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(clusterService.clusterName.value(), remoteClient)
        shards.forEach {
            val followerShardId = it.value.shardId
            if  (!retentionLeaseHelper.verifyRetentionLeaseExist(ShardId(params.remoteIndex, followerShardId.id), followerShardId)) {
                isResumable =  false
            }
        }

        if (isResumable) {
            return true
        }

        // clean up all retention leases we may have accidentally took while doing verifyRetentionLeaseExist .
        // Idempotent Op which does no harm
        shards.forEach {
            val followerShardId = it.value.shardId
            log.debug("Removing lease for $followerShardId.id ")
            retentionLeaseHelper.removeRetentionLease(ShardId(params.remoteIndex, followerShardId.id), followerShardId)
        }

        return false
    }

    private suspend fun getRemoteIndexMetadata(remoteCluster: String, remoteIndex: String): IndexMetadata {
        val remoteClusterClient = client.getRemoteClusterClient(remoteCluster).admin().cluster()
        val clusterStateRequest = remoteClusterClient.prepareState()
                .clear()
                .setIndices(remoteIndex)
                .setMetadata(true)
                .setIndicesOptions(IndicesOptions.strictSingleIndexNoExpandForbidClosed())
                .request()
        val remoteState = suspending(remoteClusterClient::state)(clusterStateRequest).state
        return remoteState.metadata.index(remoteIndex) ?: throw IndexNotFoundException("${remoteCluster}:${remoteIndex}")
    }

    private fun validateResumeReplicationRequest(request: ResumeIndexReplicationRequest) {
        val replicationStateParams = getReplicationStateParamsForIndex(clusterService, request.indexName)
                ?:
            throw IllegalArgumentException("No replication in progress for index:${request.indexName}")
        val replicationOverallState = replicationStateParams[REPLICATION_OVERALL_STATE_KEY]

        if (replicationOverallState != REPLICATION_OVERALL_STATE_PAUSED)
            throw ResourceAlreadyExistsException("Replication on Index ${request.indexName} is already running")
    }

    private suspend fun updateReplicationStateToStarted(indexName: String) {
        val replicationStateParamMap = HashMap<String, String>()
        replicationStateParamMap[REPLICATION_OVERALL_STATE_KEY] = REPLICATION_OVERALL_STATE_RUNNING_VALUE
        val updateReplicationStateDetailsRequest = UpdateReplicationStateDetailsRequest(indexName, replicationStateParamMap,
                UpdateReplicationStateDetailsRequest.UpdateType.ADD)
        submitClusterStateUpdateTask(updateReplicationStateDetailsRequest, UpdateReplicationStateDetailsTaskExecutor.INSTANCE
                as ClusterStateTaskExecutor<AcknowledgedRequest<UpdateReplicationStateDetailsRequest>>,
                clusterService,
                "resume-replication-state-params")
    }

    override fun executor(): String {
        return ThreadPool.Names.SAME
    }

    @Throws(IOException::class)
    override fun read(inp: StreamInput): AcknowledgedResponse {
        return AcknowledgedResponse(inp)
    }

    class ResumeReplicationTask(val request: ResumeIndexReplicationRequest, listener: ActionListener<AcknowledgedResponse>) :
        AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

        override fun execute(currentState: ClusterState): ClusterState {
            val newState = ClusterState.builder(currentState)

            val mdBuilder = Metadata.builder(currentState.metadata)
            val currentReplicationMetadata = currentState.metadata().custom(ReplicationMetadata.NAME)
                ?: ReplicationMetadata.EMPTY

            // add paused index setting
            val newMetadata = currentReplicationMetadata.resumeIndex(request.indexName)
            mdBuilder.putCustom(ReplicationMetadata.NAME, newMetadata)
            newState.metadata(mdBuilder)
            return newState.build()
        }

        override fun newResponse(acknowledged: Boolean) = AcknowledgedResponse(acknowledged)
    }
}
