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

package org.opensearch.replication.action.stop

import org.opensearch.replication.ReplicationPlugin.Companion.REPLICATED_INDEX_SETTING
import org.opensearch.replication.metadata.INDEX_REPLICATION_BLOCK
import org.opensearch.replication.metadata.checkIfIndexBlockedWithLevel
import org.opensearch.replication.metadata.REPLICATION_OVERALL_STATE_KEY
import org.opensearch.replication.metadata.REPLICATION_OVERALL_STATE_RUNNING_VALUE
import org.opensearch.replication.metadata.ReplicationMetadata
import org.opensearch.replication.metadata.getReplicationStateParamsForIndex
import org.opensearch.replication.util.suspending
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchException
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.indices.close.CloseIndexRequest
import org.opensearch.action.admin.indices.open.OpenIndexRequest
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.action.support.master.TransportMasterNodeAction
import org.opensearch.client.Client
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
import org.opensearch.replication.util.completeWith
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.waitForClusterStateUpdate
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import java.io.IOException

class TransportStopIndexReplicationAction @Inject constructor(transportService: TransportService,
                                                              clusterService: ClusterService,
                                                              threadPool: ThreadPool,
                                                              actionFilters: ActionFilters,
                                                              indexNameExpressionResolver:
                                                              IndexNameExpressionResolver,
                                                              val client: Client) :
    TransportMasterNodeAction<StopIndexReplicationRequest, AcknowledgedResponse> (StopIndexReplicationAction.NAME,
            transportService, clusterService, threadPool, actionFilters, ::StopIndexReplicationRequest,
            indexNameExpressionResolver), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportStopIndexReplicationAction::class.java)
    }

    override fun checkBlock(request: StopIndexReplicationRequest, state: ClusterState): ClusterBlockException? {
        try {
            checkIfIndexBlockedWithLevel(clusterService, request.indexName, ClusterBlockLevel.METADATA_WRITE)
        } catch (exception: ClusterBlockException) {
            return exception
        } catch (exception: IndexNotFoundException) {
            log.warn("Index ${request.indexName} is deleted")
        }
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    @Throws(Exception::class)
    override fun masterOperation(request: StopIndexReplicationRequest, state: ClusterState,
                                 listener: ActionListener<AcknowledgedResponse>) {
        launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            listener.completeWith {
                log.info("Stopping index replication on index:" + request.indexName)
                validateStopReplicationRequest(request)

                // Index will be deleted if replication is stopped while it is restoring.  So no need to close/reopen
                val restoring = clusterService.state().custom<RestoreInProgress>(RestoreInProgress.TYPE).any { entry ->
                    entry.indices().any { it == request.indexName }
                }
                if (!restoring &&
                        state.routingTable.hasIndex(request.indexName)) {
                    val closeResponse = suspending(client.admin().indices()::close)(CloseIndexRequest(request.indexName))
                    if (!closeResponse.isAcknowledged) {
                        throw OpenSearchException("Unable to close index: ${request.indexName}")
                    }
                }

                val stateUpdateResponse : AcknowledgedResponse =
                    clusterService.waitForClusterStateUpdate("stop_replication") { l -> StopReplicationTask(request, l)}
                if (!stateUpdateResponse.isAcknowledged) {
                    throw OpenSearchException("Failed to update cluster state")
                }

                // Index will be deleted if stop is called while it is restoring.  So no need to reopen
                if (!restoring &&
                        state.routingTable.hasIndex(request.indexName)) {
                    val reopenResponse = suspending(client.admin().indices()::open)(OpenIndexRequest(request.indexName))
                    if (!reopenResponse.isAcknowledged) {
                        throw OpenSearchException("Failed to reopen index: ${request.indexName}")
                    }
                }
                AcknowledgedResponse(true)
            }
        }
    }

    private fun validateStopReplicationRequest(request: StopIndexReplicationRequest) {
        val replicationStateParams = getReplicationStateParamsForIndex(clusterService, request.indexName)
                ?:
            throw IllegalArgumentException("No replication in progress for index:${request.indexName}")
        val replicationOverallState = replicationStateParams[REPLICATION_OVERALL_STATE_KEY]
        if (replicationOverallState == REPLICATION_OVERALL_STATE_RUNNING_VALUE)
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

            // remove replication metadata and state params
            val mdBuilder = Metadata.builder(currentState.metadata)
            val currentReplicationMetadata = currentState.metadata().custom(ReplicationMetadata.NAME)
                ?: ReplicationMetadata.EMPTY
            val clusterAlias = currentReplicationMetadata.replicatedIndices.entries.firstOrNull {
                it.value.containsKey(request.indexName)
            }?.key
            if (clusterAlias != null) {
                val newMetadata = currentReplicationMetadata.removeIndex(clusterAlias, request.indexName)
                        .removeReplicationStateParams(request.indexName)
                        .removeSecurityContext(clusterAlias, request.indexName)
                mdBuilder.putCustom(ReplicationMetadata.NAME, newMetadata)
            }

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
