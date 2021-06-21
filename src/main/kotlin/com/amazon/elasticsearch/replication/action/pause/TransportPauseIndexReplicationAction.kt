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

package com.amazon.elasticsearch.replication.action.pause

import com.amazon.elasticsearch.replication.action.replicationstatedetails.UpdateReplicationStateDetailsRequest
import com.amazon.elasticsearch.replication.metadata.*
import com.amazon.elasticsearch.replication.util.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.ResourceAlreadyExistsException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.master.AcknowledgedRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.support.master.TransportMasterNodeAction
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.AckedClusterStateUpdateTask
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.ClusterStateTaskExecutor
import org.elasticsearch.cluster.RestoreInProgress
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.block.ClusterBlockLevel
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.metadata.Metadata
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService
import java.io.IOException

class TransportPauseIndexReplicationAction @Inject constructor(transportService: TransportService,
                                                               clusterService: ClusterService,
                                                               threadPool: ThreadPool,
                                                               actionFilters: ActionFilters,
                                                               indexNameExpressionResolver:
                                                              IndexNameExpressionResolver,
                                                               val client: Client) :
    TransportMasterNodeAction<PauseIndexReplicationRequest, AcknowledgedResponse> (PauseIndexReplicationAction.NAME,
            transportService, clusterService, threadPool, actionFilters, ::PauseIndexReplicationRequest,
            indexNameExpressionResolver), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportPauseIndexReplicationAction::class.java)
    }

    override fun checkBlock(request: PauseIndexReplicationRequest, state: ClusterState): ClusterBlockException? {
        try {
            checkIfIndexBlockedWithLevel(clusterService, request.indexName, ClusterBlockLevel.METADATA_WRITE)
        } catch (exception: ClusterBlockException) {
            return exception
        }
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
                    throw ElasticsearchException("Index is in bootstrap phase currently for index:" + request.indexName)
                }

                val stateUpdateResponse : AcknowledgedResponse =
                    clusterService.waitForClusterStateUpdate("Pause_replication") { l -> PauseReplicationTask(request, l)}
                if (!stateUpdateResponse.isAcknowledged) {
                    throw ElasticsearchException("Failed to update cluster state")
                }

                updateReplicationStateToPaused(request.indexName)

                AcknowledgedResponse(true)
            }
        }
    }

    private fun validatePauseReplicationRequest(request: PauseIndexReplicationRequest) {
        val replicationStateParams = getReplicationStateParamsForIndex(clusterService, request.indexName)
                ?:
            throw IllegalArgumentException("No replication in progress for index:${request.indexName}")
        val replicationOverallState = replicationStateParams[REPLICATION_OVERALL_STATE_KEY]
        if (replicationOverallState == REPLICATION_OVERALL_STATE_PAUSED)
            throw ResourceAlreadyExistsException("Index ${request.indexName} is already paused")
        else if (replicationOverallState != REPLICATION_OVERALL_STATE_RUNNING_VALUE)
            throw IllegalStateException("Unknown value of replication state:$replicationOverallState")

    }

    override fun executor(): String {
        return ThreadPool.Names.SAME
    }

    private suspend fun updateReplicationStateToPaused(indexName: String) {
        val replicationStateParamMap = HashMap<String, String>()
        replicationStateParamMap[REPLICATION_OVERALL_STATE_KEY] = REPLICATION_OVERALL_STATE_PAUSED
        val updateReplicationStateDetailsRequest = UpdateReplicationStateDetailsRequest(indexName, replicationStateParamMap,
                UpdateReplicationStateDetailsRequest.UpdateType.ADD)
        submitClusterStateUpdateTask(updateReplicationStateDetailsRequest, UpdateReplicationStateDetailsTaskExecutor.INSTANCE
                as ClusterStateTaskExecutor<AcknowledgedRequest<UpdateReplicationStateDetailsRequest>>,
                clusterService,
                "pause-replication-state-params")
    }

    @Throws(IOException::class)
    override fun read(inp: StreamInput): AcknowledgedResponse {
        return AcknowledgedResponse(inp)
    }

    class PauseReplicationTask(val request: PauseIndexReplicationRequest, listener: ActionListener<AcknowledgedResponse>) :
        AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

        override fun execute(currentState: ClusterState): ClusterState {
            val newState = ClusterState.builder(currentState)

            val mdBuilder = Metadata.builder(currentState.metadata)
            val currentReplicationMetadata = currentState.metadata().custom(ReplicationMetadata.NAME)
                ?: ReplicationMetadata.EMPTY

            // add paused index setting
            val newMetadata = currentReplicationMetadata.pauseIndex(request.indexName, request.reason)
            mdBuilder.putCustom(ReplicationMetadata.NAME, newMetadata)
            newState.metadata(mdBuilder)
            return newState.build()
        }

        override fun newResponse(acknowledged: Boolean) = AcknowledgedResponse(acknowledged)
    }
}
