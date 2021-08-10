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

package org.opensearch.replication.action.index.block

import org.opensearch.replication.metadata.UpdateIndexBlockTask
import org.opensearch.replication.util.completeWith
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.waitForClusterStateUpdate
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchException
import org.opensearch.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.action.support.master.TransportMasterNodeAction
import org.opensearch.client.Client
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import java.io.IOException


class TransportUpddateIndexBlockAction @Inject constructor(transportService: TransportService,
                                                           clusterService: ClusterService,
                                                           threadPool: ThreadPool,
                                                           actionFilters: ActionFilters,
                                                           indexNameExpressionResolver:
                                                           IndexNameExpressionResolver,
                                                           val client: Client) :
        TransportMasterNodeAction<UpdateIndexBlockRequest, AcknowledgedResponse>(UpdateIndexBlockAction.NAME,
                transportService, clusterService, threadPool, actionFilters, ::UpdateIndexBlockRequest,
                indexNameExpressionResolver), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportUpddateIndexBlockAction::class.java)
    }

    override fun checkBlock(request: UpdateIndexBlockRequest?, state: ClusterState): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    @Throws(Exception::class)
    override fun masterOperation(request: UpdateIndexBlockRequest?, state: ClusterState?, listener: ActionListener<AcknowledgedResponse>) {
        val followerIndexName = request!!.indexName
        launch(threadPool.coroutineContext(ThreadPool.Names.MANAGEMENT)) {
            listener.completeWith { addIndexBlockForReplication(request) }
        }
    }

    private suspend fun addIndexBlockForReplication(request: UpdateIndexBlockRequest): AcknowledgedResponse {
        val addIndexBlockTaskResponse : AcknowledgedResponse =
                clusterService.waitForClusterStateUpdate("add-block") {
                    l ->
                    UpdateIndexBlockTask(request, l)
                }
        if (!addIndexBlockTaskResponse.isAcknowledged) {
            throw OpenSearchException("Failed to add index block to index:${request.indexName}")
        }
        return addIndexBlockTaskResponse
    }

    override fun executor(): String {
        return ThreadPool.Names.SAME
    }

    @Throws(IOException::class)
    override fun read(inp: StreamInput?): AcknowledgedResponse {
        return AcknowledgedResponse(inp)
    }


}
