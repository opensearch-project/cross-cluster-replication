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

package com.amazon.elasticsearch.replication.action.stop

import com.amazon.elasticsearch.replication.ReplicationPlugin.Companion.REPLICATED_INDEX_SETTING
import com.amazon.elasticsearch.replication.action.index.block.IndexBlockUpdateType
import com.amazon.elasticsearch.replication.action.index.block.UpdateIndexBlockAction
import com.amazon.elasticsearch.replication.action.index.block.UpdateIndexBlockRequest
import com.amazon.elasticsearch.replication.metadata.INDEX_REPLICATION_BLOCK
import com.amazon.elasticsearch.replication.metadata.ReplicationMetadataManager
import com.amazon.elasticsearch.replication.metadata.ReplicationOverallState
import com.amazon.elasticsearch.replication.metadata.UpdateMetadataAction
import com.amazon.elasticsearch.replication.metadata.UpdateMetadataRequest
import com.amazon.elasticsearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import com.amazon.elasticsearch.replication.metadata.state.getReplicationStateParamsForIndex
import com.amazon.elasticsearch.replication.metadata.store.ReplicationMetadata
import com.amazon.elasticsearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import com.amazon.elasticsearch.replication.task.index.IndexReplicationParams
import com.amazon.elasticsearch.replication.util.completeWith
import com.amazon.elasticsearch.replication.util.coroutineContext
import com.amazon.elasticsearch.replication.util.suspendExecute
import com.amazon.elasticsearch.replication.util.suspending
import com.amazon.elasticsearch.replication.util.waitForClusterStateUpdate
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.support.master.TransportMasterNodeAction
import org.elasticsearch.client.Client
import org.elasticsearch.client.Requests
import org.elasticsearch.cluster.AckedClusterStateUpdateTask
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.RestoreInProgress
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.block.ClusterBlockLevel
import org.elasticsearch.cluster.block.ClusterBlocks
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.metadata.Metadata
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService
import java.io.IOException

class TransportStopIndexReplicationAction @Inject constructor(transportService: TransportService,
                                                              clusterService: ClusterService,
                                                              threadPool: ThreadPool,
                                                              actionFilters: ActionFilters,
                                                              indexNameExpressionResolver:
                                                              IndexNameExpressionResolver,
                                                              val client: Client,
                                                              val replicationMetadataManager: ReplicationMetadataManager) :
    TransportMasterNodeAction<StopIndexReplicationRequest, AcknowledgedResponse> (StopIndexReplicationAction.NAME,
            transportService, clusterService, threadPool, actionFilters, ::StopIndexReplicationRequest,
            indexNameExpressionResolver), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportStopIndexReplicationAction::class.java)
    }

    override fun checkBlock(request: StopIndexReplicationRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    @Throws(Exception::class)
    override fun masterOperation(request: StopIndexReplicationRequest, state: ClusterState,
                                 listener: ActionListener<AcknowledgedResponse>) {
        launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            listener.completeWith {
                log.info("Stopping index replication on index:" + request.indexName)
                val isPaused = validateStopReplicationRequest(request)

                val updateIndexBlockRequest = UpdateIndexBlockRequest(request.indexName,IndexBlockUpdateType.REMOVE_BLOCK)
                client.suspendExecute(UpdateIndexBlockAction.INSTANCE, updateIndexBlockRequest, injectSecurityContext = true)

                // Index will be deleted if replication is stopped while it is restoring.  So no need to close/reopen
                val restoring = clusterService.state().custom<RestoreInProgress>(RestoreInProgress.TYPE, RestoreInProgress.EMPTY).any { entry ->
                    entry.indices().any { it == request.indexName }
                }
                if (!restoring &&
                        state.routingTable.hasIndex(request.indexName)) {

                    var updateRequest = UpdateMetadataRequest(request.indexName, UpdateMetadataRequest.Type.CLOSE, Requests.closeIndexRequest(request.indexName))
                    var closeResponse = client.suspendExecute(UpdateMetadataAction.INSTANCE, updateRequest, injectSecurityContext = true)
                    if (!closeResponse.isAcknowledged) {
                        throw ElasticsearchException("Unable to close index: ${request.indexName}")
                    }
                }
                val replMetadata = replicationMetadataManager.getIndexReplicationMetadata(request.indexName)
                // If paused , we need to make attempt to clear retention leases as Shard Tasks are non-existent
                if (isPaused) {
                    attemptRemoveRetentionLease(replMetadata, request.indexName)
                }

                val clusterStateUpdateResponse : AcknowledgedResponse =
                    clusterService.waitForClusterStateUpdate("stop_replication") { l -> StopReplicationTask(request, l)}
                if (!clusterStateUpdateResponse.isAcknowledged) {
                    throw ElasticsearchException("Failed to update cluster state")
                }

                // Index will be deleted if stop is called while it is restoring.  So no need to reopen
                if (!restoring &&
                        state.routingTable.hasIndex(request.indexName)) {
                    val reopenResponse = client.suspending(client.admin().indices()::open, injectSecurityContext = true)(OpenIndexRequest(request.indexName))
                    if (!reopenResponse.isAcknowledged) {
                        throw ElasticsearchException("Failed to reopen index: ${request.indexName}")
                    }
                }
                replicationMetadataManager.deleteIndexReplicationMetadata(request.indexName)
                AcknowledgedResponse(true)
            }
        }
    }

    private suspend fun attemptRemoveRetentionLease(replMetadata: ReplicationMetadata, followerIndexName: String) {
        try {
            val remoteMetadata = getLeaderIndexMetadata(replMetadata.connectionName, replMetadata.leaderContext.resource)
            val params = IndexReplicationParams(replMetadata.connectionName, remoteMetadata.index, followerIndexName)
            val remoteClient = client.getRemoteClusterClient(params.leaderAlias)
            val shards = clusterService.state().routingTable.indicesRouting().get(params.followerIndexName).shards()
            val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(clusterService.clusterName.value(), remoteClient)
            shards.forEach {
                val followerShardId = it.value.shardId
                log.debug("Removing lease for $followerShardId.id ")
                retentionLeaseHelper.attemptRetentionLeaseRemoval(ShardId(params.leaderIndex, followerShardId.id), followerShardId)
            }
        } catch (e: Exception) {
            log.error("Exception while trying to remove Retention Lease ", e )
        }
    }

    private suspend fun getLeaderIndexMetadata(leaderAlias: String, leaderIndex: String): IndexMetadata {
        val leaderClusterClient = client.getRemoteClusterClient(leaderAlias)
        val clusterStateRequest = leaderClusterClient.admin().cluster().prepareState()
                .clear()
                .setIndices(leaderIndex)
                .setMetadata(true)
                .setIndicesOptions(IndicesOptions.strictSingleIndexNoExpandForbidClosed())
                .request()
        val leaderState = leaderClusterClient.suspending(leaderClusterClient.admin().cluster()::state)(clusterStateRequest).state
        return leaderState.metadata.index(leaderIndex) ?: throw IndexNotFoundException("${leaderAlias}:${leaderIndex}")
    }

    private fun validateStopReplicationRequest(request: StopIndexReplicationRequest): Boolean {
        val replicationStateParams = getReplicationStateParamsForIndex(clusterService, request.indexName)
                ?:
            throw IllegalArgumentException("No replication in progress for index:${request.indexName}")
        val replicationOverallState = replicationStateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE]
        if (replicationOverallState == ReplicationOverallState.RUNNING.name)
            return false
        else if (replicationOverallState == ReplicationOverallState.PAUSED.name)
            return true
        throw IllegalStateException("Unknown value of replication state:$replicationOverallState")
    }

    override fun executor(): String {
        return ThreadPool.Names.SAME
    }

    @Throws(IOException::class)
    override fun read(inp: StreamInput): AcknowledgedResponse {
        return AcknowledgedResponse(inp)
    }

    class StopReplicationTask(val request: StopIndexReplicationRequest, listener: ActionListener<AcknowledgedResponse>) :
        AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

        override fun execute(currentState: ClusterState): ClusterState {
            val newState = ClusterState.builder(currentState)

            // remove index block
            if (currentState.blocks.hasIndexBlock(request.indexName, INDEX_REPLICATION_BLOCK)) {
                val newBlocks = ClusterBlocks.builder().blocks(currentState.blocks)
                    .removeIndexBlock(request.indexName, INDEX_REPLICATION_BLOCK)
                newState.blocks(newBlocks)
            }

            val mdBuilder = Metadata.builder(currentState.metadata)
            // remove replicated index setting
            val currentIndexMetadata = currentState.metadata.index(request.indexName)
            if (currentIndexMetadata != null) {
                val newIndexMetadata = IndexMetadata.builder(currentIndexMetadata)
                        .settings(Settings.builder().put(currentIndexMetadata.settings).putNull(REPLICATED_INDEX_SETTING.key))
                        .settingsVersion(1 + currentIndexMetadata.settingsVersion)
                mdBuilder.put(newIndexMetadata)
            }
            newState.metadata(mdBuilder)
            return newState.build()
        }

        override fun newResponse(acknowledged: Boolean) = AcknowledgedResponse(acknowledged)
    }
}
