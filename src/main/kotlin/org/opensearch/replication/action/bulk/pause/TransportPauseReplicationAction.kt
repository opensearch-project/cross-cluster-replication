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

package org.opensearch.replication.action.bulk.pause

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
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.persistent.PersistentTasksCustomMetadata
import org.opensearch.replication.task.bulk.BulkExecuter
import org.opensearch.replication.task.bulk.BulkParams
import org.opensearch.replication.util.*
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import java.io.IOException

class TransportPauseReplicationAction @Inject constructor(transportService: TransportService,
                                                              clusterService: ClusterService,
                                                              threadPool: ThreadPool,
                                                              actionFilters: ActionFilters,
                                                              indexNameExpressionResolver:
                                                              IndexNameExpressionResolver,
                                                              val client: Client,
                                                              val replicationMetadataManager: ReplicationMetadataManager) :
    TransportMasterNodeAction<BulkPauseReplicationRequest, AcknowledgedResponse> (
        BulkPauseReplicationAction.NAME,
        transportService, clusterService, threadPool, actionFilters, ::BulkPauseReplicationRequest,
        indexNameExpressionResolver), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportPauseReplicationAction::class.java)
    }

    override fun checkBlock(request: BulkPauseReplicationRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    @Throws(Exception::class)
    override fun masterOperation(request: BulkPauseReplicationRequest, state: ClusterState,
                                 listener: ActionListener<AcknowledgedResponse>) {
        launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            try {
                log.info("Will work on ${request.indexPattern}")
                var indicesList = clusterService.state().metadata().concreteAllIndices.asIterable()
                indicesList = indicesList.toList()
                log.info(indicesList)

                log.info("Pattern is ${request.indexPattern}")
                startBulkTaskPause(request.indexPattern, "test")

                //TODO return taskid
                listener.onResponse(AcknowledgedResponse(true, ))
//
//
            } catch (e: Exception) {
//                log.error("Stop replication failed for index[${request.indexName}] with error ${e.stackTraceToString()}")
                listener.onFailure(e)
            }
        }
    }


    private suspend fun startBulkTaskPause(patternName: String, clusterName: String): PersistentTasksCustomMetadata.PersistentTask<BulkParams> {
        val response = persistentTasksService.startTask("BulkkCrossClusterReplicationTask",
            BulkExecuter.TASK_NAME,
            BulkParams("PAUSE", patternName)
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



}
