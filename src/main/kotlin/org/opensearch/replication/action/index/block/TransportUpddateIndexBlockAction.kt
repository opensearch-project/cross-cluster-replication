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

package org.opensearch.replication.action.index.block

import org.opensearch.replication.metadata.UpdateIndexBlockTaskExecutor
import org.opensearch.replication.util.completeWith
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.waitForClusterStateUpdate
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchException
import org.opensearch.core.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction
import org.opensearch.transport.client.Client
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.core.common.io.stream.StreamInput
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
    TransportClusterManagerNodeAction<UpdateIndexBlockRequest, AcknowledgedResponse>(UpdateIndexBlockAction.NAME,
                transportService, clusterService, threadPool, actionFilters, ::UpdateIndexBlockRequest,
                indexNameExpressionResolver), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportUpddateIndexBlockAction::class.java)
    }

    override fun checkBlock(request: UpdateIndexBlockRequest?, state: ClusterState): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    @Throws(Exception::class)
    override fun clusterManagerOperation(request: UpdateIndexBlockRequest?, state: ClusterState?, listener: ActionListener<AcknowledgedResponse>) {
        val followerIndexName = request!!.indexName
        log.debug("${request.updateType} index block for $followerIndexName")
        launch(threadPool.coroutineContext(ThreadPool.Names.MANAGEMENT)) {
            listener.completeWith { updateIndexBlockForReplication(request) }
        }
    }

    private suspend fun updateIndexBlockForReplication(request: UpdateIndexBlockRequest): AcknowledgedResponse {
        // Shared singleton executor: concurrent block-add / block-remove submissions coalesce into
        // one cluster-manager update turn via TaskBatcher (batching key is the executor's identity).
        val response = clusterService.waitForClusterStateUpdate(
                source = "update-block:${request.updateType}",
                task = request,
                executor = UpdateIndexBlockTaskExecutor,
                ackTimeout = request.ackTimeout()
        )
        if (!response.isAcknowledged) {
            throw OpenSearchException("Failed to ${request.updateType} index block on ${request.indexName}")
        }
        return response
    }

    override fun executor(): String {
        return ThreadPool.Names.SAME
    }

    @Throws(IOException::class)
    override fun read(inp: StreamInput?): AcknowledgedResponse {
        return AcknowledgedResponse(inp)
    }


}
