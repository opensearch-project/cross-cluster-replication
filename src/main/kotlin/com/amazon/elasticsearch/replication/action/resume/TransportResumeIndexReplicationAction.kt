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
import com.amazon.elasticsearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import com.amazon.elasticsearch.replication.metadata.state.getReplicationStateParamsForIndex
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
                                                                indexNameExpressionResolver: IndexNameExpressionResolver,
                                                                val client: Client,
                                                                val replicationMetadataManager: ReplicationMetadataManager) :
    TransportMasterNodeAction<ResumeIndexReplicationRequest, AcknowledgedResponse> (ResumeIndexReplicationAction.NAME,
            transportService, clusterService, threadPool, actionFilters, ::ResumeIndexReplicationRequest,
            indexNameExpressionResolver), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportResumeIndexReplicationAction::class.java)
    }

    override fun checkBlock(request: ResumeIndexReplicationRequest, state: ClusterState): ClusterBlockException? {
        try {
            checkIfIndexBlockedWithLevel(clusterService, request.indexName, ClusterBlockLevel.METADATA_WRITE)
        } catch (exception: ClusterBlockException) {
            return exception
        }
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    @Throws(Exception::class)
    override fun masterOperation(request: ResumeIndexReplicationRequest, state: ClusterState,
                                 listener: ActionListener<AcknowledgedResponse>) {
        launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            listener.completeWith {
                log.info("Resuming index replication on index:" + request.indexName)
                validateResumeReplicationRequest(request)
                val replMetdata = replicationMetadataManager.getIndexReplicationMetadata(request.indexName)
                val remoteMetadata = getRemoteIndexMetadata(replMetdata.connectionName, replMetdata.leaderContext.resource)
                val params = IndexReplicationParams(replMetdata.connectionName, remoteMetadata.index, request.indexName)
                if (!isResumable(params)) {
                    throw ResourceNotFoundException("Retention lease doesn't exist. Replication can't be resumed for ${request.indexName}")
                }
                replicationMetadataManager.updateIndexReplicationState(request.indexName, ReplicationOverallState.RUNNING)
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
        val remoteClient = client.getRemoteClusterClient(params.remoteCluster)
        val shards = clusterService.state().routingTable.indicesRouting().get(params.followerIndexName).shards()
        val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(clusterService.clusterName.value(), remoteClient)
        shards.forEach {
            val followerShardId = it.value.shardId
            if  (!retentionLeaseHelper.verifyRetentionLeaseExist(ShardId(params.remoteIndex, followerShardId.id), followerShardId)) {
                return  false
            }
        }
        return true
    }

    private suspend fun getRemoteIndexMetadata(remoteCluster: String, remoteIndex: String): IndexMetadata {
        val remoteClusterClient = client.getRemoteClusterClient(remoteCluster)
        val clusterStateRequest = remoteClusterClient.admin().cluster().prepareState()
                .clear()
                .setIndices(remoteIndex)
                .setMetadata(true)
                .setIndicesOptions(IndicesOptions.strictSingleIndexNoExpandForbidClosed())
                .request()
        val remoteState = remoteClusterClient.suspending(remoteClusterClient.admin().cluster()::state)(clusterStateRequest).state
        return remoteState.metadata.index(remoteIndex) ?: throw IndexNotFoundException("${remoteCluster}:${remoteIndex}")
    }

    private fun validateResumeReplicationRequest(request: ResumeIndexReplicationRequest) {
        val replicationStateParams = getReplicationStateParamsForIndex(clusterService, request.indexName)
                ?:
            throw IllegalArgumentException("No replication in progress for index:${request.indexName}")
        val replicationOverallState = replicationStateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE]

        if (replicationOverallState != ReplicationOverallState.PAUSED.name)
            throw ResourceAlreadyExistsException("Replication on Index ${request.indexName} is already running")
    }

    override fun executor(): String {
        return ThreadPool.Names.SAME
    }

    @Throws(IOException::class)
    override fun read(inp: StreamInput): AcknowledgedResponse {
        return AcknowledgedResponse(inp)
    }
}
