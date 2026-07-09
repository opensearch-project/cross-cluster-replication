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

package org.opensearch.replication.action.replicationstatedetails

import org.opensearch.replication.metadata.UpdateReplicationStateDetailsTaskExecutor
import org.opensearch.replication.util.completeWith
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.submitClusterStateUpdateTask
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.opensearch.core.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.clustermanager.AcknowledgedRequest
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.ClusterStateTaskExecutor
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService

class TransportUpdateReplicationStateDetails @Inject constructor(transportService: TransportService,
                                                                 clusterService: ClusterService,
                                                                 threadPool: ThreadPool,
                                                                 actionFilters: ActionFilters,
                                                                 indexNameExpressionResolver: IndexNameExpressionResolver) :
    TransportClusterManagerNodeAction<UpdateReplicationStateDetailsRequest, AcknowledgedResponse>(UpdateReplicationStateAction.NAME,
                transportService, clusterService, threadPool, actionFilters, ::UpdateReplicationStateDetailsRequest, indexNameExpressionResolver),
        CoroutineScope by GlobalScope {

    override fun checkBlock(request: UpdateReplicationStateDetailsRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks.globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    override fun clusterManagerOperation(request: UpdateReplicationStateDetailsRequest, state: ClusterState,
                                 listener: ActionListener<AcknowledgedResponse>) {

        launch(threadPool.coroutineContext(ThreadPool.Names.MANAGEMENT)) {
            listener.completeWith {
                submitClusterStateUpdateTask(request, UpdateReplicationStateDetailsTaskExecutor.INSTANCE
                        as ClusterStateTaskExecutor<AcknowledgedRequest<UpdateReplicationStateDetailsRequest>>,
                        clusterService,
                        "update-replication-state-params")
                AcknowledgedResponse(true)
            }
        }
    }

    override fun executor(): String {
        return ThreadPool.Names.SAME
    }

    override fun read(inp: StreamInput): AcknowledgedResponse {
        return AcknowledgedResponse(inp)
    }
}
