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

package org.opensearch.replication.action.bulk.stop

import org.opensearch.replication.metadata.ReplicationMetadataManager
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.action.support.master.TransportMasterNodeAction
import org.opensearch.client.Client
import org.opensearch.cluster.AckedClusterStateUpdateTask
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.persistent.AllocatedPersistentTask
import org.opensearch.persistent.PersistentTasksCustomMetadata
import org.opensearch.replication.task.bulk.BulkExecuter
import org.opensearch.replication.task.bulk.BulkParams
import org.opensearch.replication.util.*
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import java.io.IOException

class TransportBulkStopReplicationAction @Inject constructor(transportService: TransportService,
                                                              clusterService: ClusterService,
                                                              threadPool: ThreadPool,
                                                              actionFilters: ActionFilters,
                                                              indexNameExpressionResolver:
                                                              IndexNameExpressionResolver,
                                                              val client: Client,
                                                              val replicationMetadataManager: ReplicationMetadataManager) :
    TransportMasterNodeAction<BulkStopReplicationRequest, AcknowledgedResponse> (
        BulkStopReplicationAction.NAME,
        transportService, clusterService, threadPool, actionFilters, ::BulkStopReplicationRequest,
        indexNameExpressionResolver), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportBulkStopReplicationAction::class.java)
    }

    override fun checkBlock(request: BulkStopReplicationRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    @Throws(Exception::class)
    override fun masterOperation(request: BulkStopReplicationRequest, state: ClusterState,
                                 listener: ActionListener<AcknowledgedResponse>) {
        launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            try {
//                log.info("Stopping index replication on index:" + request.indexName)
                log.info("Will work on ${request.indexPattern}")
                var indicesList = clusterService.state().metadata().concreteAllIndices.asIterable()
                indicesList = indicesList.toList()
                log.info(indicesList)
                 startBulkTask(request.indexPattern, "test")

                listener.onResponse(AcknowledgedResponse(true, ))

            } catch (e: Exception) {
//                log.error("Stop replication failed for index[${request.indexName}] with error ${e.stackTraceToString()}")
                listener.onFailure(e)
            }
        }
    }


    private suspend fun startBulkTask(patternName: String, clusterName: String): PersistentTasksCustomMetadata.PersistentTask<BulkParams> {

        val response = persistentTasksService.startTask("BulkkCrossClusterReplicationTask",
            BulkExecuter.TASK_NAME,
            BulkParams( "STOP", patternName)
        )

        return response
    }

    override fun executor(): String {
        return ThreadPool.Names.SAME
    }

    @Throws(IOException::class)
    override fun read(inp: StreamInput): AcknowledgedResponse {
        return AcknowledgedResponse(inp)
    }

    class StopReplicationTask(val request: BulkStopReplicationRequest, listener: ActionListener<AcknowledgedResponse>) :
        AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

        override fun execute(currentState: ClusterState): ClusterState {
            val newState = ClusterState.builder(currentState)

            return newState.build()
        }

        override fun newResponse(acknowledged: Boolean) = AcknowledgedResponse(acknowledged)
    }
}
