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

package org.opensearch.replication.action.pause

import org.opensearch.replication.metadata.*
import org.opensearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import org.opensearch.replication.metadata.state.getReplicationStateParamsForIndex
import org.opensearch.replication.util.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchException
import org.opensearch.ResourceAlreadyExistsException
import org.opensearch.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.master.AcknowledgedRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.action.support.master.TransportMasterNodeAction
import org.opensearch.client.Client
import org.opensearch.cluster.AckedClusterStateUpdateTask
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.ClusterStateTaskExecutor
import org.opensearch.cluster.RestoreInProgress
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import java.io.IOException

class TransportPauseIndexReplicationAction @Inject constructor(transportService: TransportService,
                                                               clusterService: ClusterService,
                                                               threadPool: ThreadPool,
                                                               actionFilters: ActionFilters,
                                                               indexNameExpressionResolver:
                                                               IndexNameExpressionResolver,
                                                               val client: Client,
                                                               val replicationMetadataManager: ReplicationMetadataManager) :
    TransportMasterNodeAction<PauseIndexReplicationRequest, AcknowledgedResponse> (PauseIndexReplicationAction.NAME,
            transportService, clusterService, threadPool, actionFilters, ::PauseIndexReplicationRequest,
            indexNameExpressionResolver), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportPauseIndexReplicationAction::class.java)
    }

    override fun checkBlock(request: PauseIndexReplicationRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    @Throws(Exception::class)
    override fun masterOperation(request: PauseIndexReplicationRequest, state: ClusterState,
                                 listener: ActionListener<AcknowledgedResponse>) {
        launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            listener.completeWith {
                log.info("Pausing index replication on index:" + request.indexName)
                validatePauseReplicationRequest(request)

                // Restoring Index can't be paused
                val restoring = clusterService.state().custom<RestoreInProgress>(RestoreInProgress.TYPE).any { entry ->
                    entry.indices().any { it == request.indexName }
                }

                if (restoring) {
                    throw OpenSearchException("Index is in restore phase currently for index: ${request.indexName}. You can pause after restore completes." )
                }

                // If the index is not in bootstrap phase, bring down the tasks and persist the info
                replicationMetadataManager.updateIndexReplicationState(request.indexName, ReplicationOverallState.PAUSED, request.reason)

                AcknowledgedResponse(true)
            }
        }
    }

    private fun validatePauseReplicationRequest(request: PauseIndexReplicationRequest) {
        val replicationStateParams = getReplicationStateParamsForIndex(clusterService, request.indexName)
                ?:
            throw IllegalArgumentException("No replication in progress for index:${request.indexName}")
        val replicationOverallState = replicationStateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE]
        if (replicationOverallState == ReplicationOverallState.PAUSED.name)
            throw ResourceAlreadyExistsException("Index ${request.indexName} is already paused")
        else if (replicationOverallState != ReplicationOverallState.RUNNING.name)
            throw IllegalStateException("Unknown value of replication state:$replicationOverallState")
    }

    override fun executor(): String {
        return ThreadPool.Names.SAME
    }

    @Throws(IOException::class)
    override fun read(inp: StreamInput): AcknowledgedResponse {
        return AcknowledgedResponse(inp)
    }
}
