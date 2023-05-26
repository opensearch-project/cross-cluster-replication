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

import com.amazon.elasticsearch.replication.ReplicationPlugin.Companion.KNN_INDEX_SETTING
import com.amazon.elasticsearch.replication.action.index.ReplicateIndexResponse
import com.amazon.elasticsearch.replication.metadata.ReplicationMetadataManager
import com.amazon.elasticsearch.replication.metadata.ReplicationOverallState
import com.amazon.elasticsearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import com.amazon.elasticsearch.replication.metadata.state.getReplicationStateParamsForIndex
import com.amazon.elasticsearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import com.amazon.elasticsearch.replication.task.ReplicationState
import com.amazon.elasticsearch.replication.task.index.IndexReplicationExecutor
import com.amazon.elasticsearch.replication.task.index.IndexReplicationParams
import com.amazon.elasticsearch.replication.task.index.IndexReplicationState
import com.amazon.elasticsearch.replication.util.ValidationUtil
import com.amazon.elasticsearch.replication.util.completeWith
import com.amazon.elasticsearch.replication.util.coroutineContext
import com.amazon.elasticsearch.replication.util.persistentTasksService
import com.amazon.elasticsearch.replication.util.startTask
import com.amazon.elasticsearch.replication.util.suspending
import com.amazon.elasticsearch.replication.util.waitForTaskCondition
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ResourceAlreadyExistsException
import org.elasticsearch.ResourceNotFoundException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.support.master.TransportMasterNodeAction
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.block.ClusterBlockLevel
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.env.Environment
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService
import com.amazon.elasticsearch.replication.util.indicesService
import java.io.IOException

class TransportResumeIndexReplicationAction @Inject constructor(transportService: TransportService,
                                                                clusterService: ClusterService,
                                                                threadPool: ThreadPool,
                                                                actionFilters: ActionFilters,
                                                                indexNameExpressionResolver: IndexNameExpressionResolver,
                                                                val client: Client,
                                                                val replicationMetadataManager: ReplicationMetadataManager,
                                                                private val environment: Environment) :
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
                val replMetdata = replicationMetadataManager.getIndexReplicationMetadata(request.indexName)
                val remoteMetadata = getLeaderIndexMetadata(replMetdata.connectionName, replMetdata.leaderContext.resource)
                val params = IndexReplicationParams(replMetdata.connectionName, remoteMetadata.index, request.indexName)
                if (!isResumable(params)) {
                    throw ResourceNotFoundException("Retention lease doesn't exist. Replication can't be resumed for ${request.indexName}")
                }

                val remoteClient = client.getRemoteClusterClient(params.leaderAlias)
                val getSettingsRequest = GetSettingsRequest().includeDefaults(false).indices(params.leaderIndex.name)
                val settingsResponse = remoteClient.suspending(
                    remoteClient.admin().indices()::getSettings,
                    injectSecurityContext = true
                )(getSettingsRequest)

                val leaderSettings = settingsResponse.indexToSettings.get(params.leaderIndex.name) ?: throw IndexNotFoundException(params.leaderIndex.name)

                // k-NN Setting is a static setting. In case the setting is changed at the leader index before resume,
                // block the resume.
                if(leaderSettings.getAsBoolean(KNN_INDEX_SETTING, false)) {
                    throw IllegalStateException("Index setting (index.knn) changed. Cannot resume k-NN index - ${params.leaderIndex.name}")
                }

                ValidationUtil.validateAnalyzerSettings(environment, leaderSettings, replMetdata.settings)

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
        var isResumable = true
        val remoteClient = client.getRemoteClusterClient(params.leaderAlias)
        val shards = clusterService.state().routingTable.indicesRouting().get(params.followerIndexName).shards()
        val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(clusterService.clusterName.value(), clusterService.state().metadata.clusterUUID(), remoteClient)
        shards.forEach {
            val followerShardId = it.value.shardId

            val followerIndexService = indicesService.indexServiceSafe(followerShardId.index)
            val indexShard = followerIndexService.getShard(followerShardId.id)

            if  (!retentionLeaseHelper.verifyRetentionLeaseExist(ShardId(params.leaderIndex, followerShardId.id), followerShardId, indexShard.lastSyncedGlobalCheckpoint+1)) {
                isResumable = false
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
            retentionLeaseHelper.attemptRetentionLeaseRemoval(ShardId(params.leaderIndex, followerShardId.id), followerShardId)
        }

        return false
    }

    private suspend fun getLeaderIndexMetadata(leaderAlias: String, leaderIndex: String): IndexMetadata {
        val leaderClusterClient = client.getRemoteClusterClient(leaderAlias)
        val clusterStateRequest = leaderClusterClient.admin().cluster().prepareState()
                .clear()
                .setIndices(leaderIndex)
                .setMetadata(true)
                .setIndicesOptions(IndicesOptions.strictSingleIndexNoExpandForbidClosed())
                .request()
        val remoteState = leaderClusterClient.suspending(leaderClusterClient.admin().cluster()::state)(clusterStateRequest).state
        return remoteState.metadata.index(leaderIndex) ?: throw IndexNotFoundException("${leaderAlias}:${leaderIndex}")
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
