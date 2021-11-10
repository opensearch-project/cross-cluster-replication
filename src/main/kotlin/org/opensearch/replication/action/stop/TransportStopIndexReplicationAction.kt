/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.replication.action.stop

import org.opensearch.replication.ReplicationPlugin.Companion.REPLICATED_INDEX_SETTING
import org.opensearch.replication.action.index.block.IndexBlockUpdateType
import org.opensearch.replication.action.index.block.UpdateIndexBlockAction
import org.opensearch.replication.action.index.block.UpdateIndexBlockRequest
import org.opensearch.replication.metadata.INDEX_REPLICATION_BLOCK
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.metadata.ReplicationOverallState
import org.opensearch.replication.metadata.UpdateMetadataAction
import org.opensearch.replication.metadata.UpdateMetadataRequest
import org.opensearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import org.opensearch.replication.metadata.state.getReplicationStateParamsForIndex
import org.opensearch.replication.metadata.store.ReplicationMetadata
import org.opensearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import org.opensearch.replication.task.index.IndexReplicationParams
import org.opensearch.replication.util.completeWith
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.suspendExecute
import org.opensearch.replication.util.suspending
import org.opensearch.replication.util.waitForClusterStateUpdate
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchException
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.indices.open.OpenIndexRequest
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.action.support.master.TransportMasterNodeAction
import org.opensearch.client.Client
import org.opensearch.client.Requests
import org.opensearch.cluster.AckedClusterStateUpdateTask
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.RestoreInProgress
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.block.ClusterBlocks
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.settings.Settings
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.shard.ShardId
import org.opensearch.replication.util.stackTraceToString
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
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
            try {
                log.info("Stopping index replication on index:" + request.indexName)

                // NOTE: We remove the block first before validation since it is harmless idempotent operations and
                //       gives back control of the index even if any failure happens in one of the steps post this.
                val updateIndexBlockRequest = UpdateIndexBlockRequest(request.indexName,IndexBlockUpdateType.REMOVE_BLOCK)
                val updateIndexBlockResponse = client.suspendExecute(UpdateIndexBlockAction.INSTANCE, updateIndexBlockRequest, injectSecurityContext = true)
                if(!updateIndexBlockResponse.isAcknowledged) {
                    throw OpenSearchException("Failed to remove index block on ${request.indexName}")
                }

                validateStopReplicationRequest(request)

                // Index will be deleted if replication is stopped while it is restoring.  So no need to close/reopen
                val restoring = clusterService.state().custom<RestoreInProgress>(RestoreInProgress.TYPE, RestoreInProgress.EMPTY).any { entry ->
                    entry.indices().any { it == request.indexName }
                }
                if(restoring) {
                    log.info("Index[${request.indexName}] is in restoring stage")
                }
                if (!restoring &&
                        state.routingTable.hasIndex(request.indexName)) {

                    var updateRequest = UpdateMetadataRequest(request.indexName, UpdateMetadataRequest.Type.CLOSE, Requests.closeIndexRequest(request.indexName))
                    var closeResponse = client.suspendExecute(UpdateMetadataAction.INSTANCE, updateRequest, injectSecurityContext = true)
                    if (!closeResponse.isAcknowledged) {
                        throw OpenSearchException("Unable to close index: ${request.indexName}")
                    }
                }
                val replMetadata = replicationMetadataManager.getIndexReplicationMetadata(request.indexName)
                try {
                    val remoteClient = client.getRemoteClusterClient(replMetadata.connectionName)
                    val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(clusterService.clusterName.value(), remoteClient)
                    retentionLeaseHelper.attemptRemoveRetentionLease(clusterService, replMetadata, request.indexName)
                } catch(e: Exception) {
                    log.error("Failed to remove retention lease from the leader cluster", e)
                }

                val clusterStateUpdateResponse : AcknowledgedResponse =
                    clusterService.waitForClusterStateUpdate("stop_replication") { l -> StopReplicationTask(request, l)}
                if (!clusterStateUpdateResponse.isAcknowledged) {
                    throw OpenSearchException("Failed to update cluster state")
                }

                // Index will be deleted if stop is called while it is restoring.  So no need to reopen
                if (!restoring &&
                        state.routingTable.hasIndex(request.indexName)) {
                    val reopenResponse = client.suspending(client.admin().indices()::open, injectSecurityContext = true)(OpenIndexRequest(request.indexName))
                    if (!reopenResponse.isAcknowledged) {
                        throw OpenSearchException("Failed to reopen index: ${request.indexName}")
                    }
                }
                replicationMetadataManager.deleteIndexReplicationMetadata(request.indexName)
                listener.onResponse(AcknowledgedResponse(true))
            } catch (e: Exception) {
                log.error("Stop replication failed for index[${request.indexName}] with error ${e.stackTraceToString()}")
                listener.onFailure(e)
            }
        }
    }

    private fun validateStopReplicationRequest(request: StopIndexReplicationRequest) {
        val replicationStateParams = getReplicationStateParamsForIndex(clusterService, request.indexName)
                ?:
            throw IllegalArgumentException("No replication in progress for index:${request.indexName}")
        val replicationOverallState = replicationStateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE]
        if (replicationOverallState == ReplicationOverallState.RUNNING.name ||
            replicationOverallState == ReplicationOverallState.STOPPED.name ||
            replicationOverallState == ReplicationOverallState.FAILED.name ||
            replicationOverallState == ReplicationOverallState.PAUSED.name)
            return
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
