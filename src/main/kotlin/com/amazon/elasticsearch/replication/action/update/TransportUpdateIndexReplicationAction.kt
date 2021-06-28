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

package com.amazon.elasticsearch.replication.action.update

import com.amazon.elasticsearch.replication.metadata.checkIfIndexBlockedWithLevel
import com.amazon.elasticsearch.replication.metadata.REPLICATION_OVERALL_STATE_KEY
import com.amazon.elasticsearch.replication.metadata.REPLICATION_OVERALL_STATE_RUNNING_VALUE
import com.amazon.elasticsearch.replication.metadata.ReplicationMetadata
import com.amazon.elasticsearch.replication.metadata.getReplicationStateParamsForIndex
import com.amazon.elasticsearch.replication.util.completeWith
import com.amazon.elasticsearch.replication.util.coroutineContext
import com.amazon.elasticsearch.replication.util.waitForClusterStateUpdate
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.support.master.TransportMasterNodeAction
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.AckedClusterStateUpdateTask
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.block.ClusterBlockLevel
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.metadata.Metadata
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService
import java.io.IOException

class TransportUpdateIndexReplicationAction @Inject constructor(transportService: TransportService,
                                                              clusterService: ClusterService,
                                                              threadPool: ThreadPool,
                                                              actionFilters: ActionFilters,
                                                              indexNameExpressionResolver:
                                                              IndexNameExpressionResolver,
                                                              val client: Client) :
    TransportMasterNodeAction<UpdateIndexReplicationRequest, AcknowledgedResponse> (UpdateIndexReplicationAction.NAME,
            transportService, clusterService, threadPool, actionFilters, ::UpdateIndexReplicationRequest,
            indexNameExpressionResolver), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportUpdateIndexReplicationAction::class.java)
    }

    override fun checkBlock(request: UpdateIndexReplicationRequest, state: ClusterState): ClusterBlockException? {
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
    override fun masterOperation(request: UpdateIndexReplicationRequest, state: ClusterState,
                                 listener: ActionListener<AcknowledgedResponse>) {
        launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            listener.completeWith {
                log.info("Updating index replication on index:" + request.indexName)
                validateUpdateReplicationRequest(request)

                val stateUpdateResponse : AcknowledgedResponse =
                    clusterService.waitForClusterStateUpdate("update_replication") { l -> UpdateReplicationTask(request, l)}
                if (!stateUpdateResponse.isAcknowledged) {
                    throw ElasticsearchException("Failed to update cluster state")
                }

                AcknowledgedResponse(true)
            }
        }
    }

    private fun validateUpdateReplicationRequest(request: UpdateIndexReplicationRequest) {
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

    class UpdateReplicationTask(val request: UpdateIndexReplicationRequest, listener: ActionListener<AcknowledgedResponse>) :
        AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

        override fun execute(currentState: ClusterState): ClusterState {
            val newState = ClusterState.builder(currentState)

            val mdBuilder = Metadata.builder(currentState.metadata)
            val currentReplicationMetadata = currentState.metadata().custom(ReplicationMetadata.NAME)
                ?: ReplicationMetadata.EMPTY

            val newMetadata = currentReplicationMetadata.addSetting(request.indexName, request.settings)
            mdBuilder.putCustom(ReplicationMetadata.NAME, newMetadata)
            newState.metadata(mdBuilder)
            return newState.build()
        }

        override fun newResponse(acknowledged: Boolean) = AcknowledgedResponse(acknowledged)
    }
}
