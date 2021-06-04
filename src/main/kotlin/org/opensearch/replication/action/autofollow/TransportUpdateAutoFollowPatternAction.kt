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

package org.opensearch.replication.action.autofollow

import org.opensearch.replication.metadata.ReplicationMetadata
import org.opensearch.replication.metadata.UpdateAutoFollowPattern
import org.opensearch.replication.task.autofollow.AutoFollowExecutor
import org.opensearch.replication.task.autofollow.AutoFollowParams
import org.opensearch.replication.util.SecurityContext
import org.opensearch.replication.util.persistentTasksService
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchException
import org.opensearch.ResourceAlreadyExistsException
import org.opensearch.ResourceNotFoundException
import org.opensearch.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.action.support.master.TransportMasterNodeAction
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.replication.util.completeWith
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.removeTask
import org.opensearch.replication.util.startTask
import org.opensearch.replication.util.waitForClusterStateUpdate
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService

class TransportUpdateAutoFollowPatternAction @Inject constructor(
    transportService: TransportService, clusterService: ClusterService, threadPool: ThreadPool,
    actionFilters: ActionFilters, indexNameExpressionResolver: IndexNameExpressionResolver,
    private val client: NodeClient) : TransportMasterNodeAction<UpdateAutoFollowPatternRequest, AcknowledgedResponse>(
    UpdateAutoFollowPatternAction.NAME, true, transportService, clusterService, threadPool, actionFilters,
    ::UpdateAutoFollowPatternRequest, indexNameExpressionResolver), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportUpdateAutoFollowPatternAction::class.java)
        const val AUTOFOLLOW_EXCEPTION_GENERIC_STRING = "Failed to update autofollow pattern"
    }

    override fun executor(): String = ThreadPool.Names.SAME

    override fun read(inp: StreamInput) = AcknowledgedResponse(inp)

    override fun masterOperation(request: UpdateAutoFollowPatternRequest, state: ClusterState,
                                 listener: ActionListener<AcknowledgedResponse>) {
        // simplest way to check if there's a connection with the given name. Throws NoSuchRemoteClusterException if not..
        try {
            client.getRemoteClusterClient(request.connection)
        } catch (e : Exception) {
            listener.onFailure(e)
            return
        }

        launch(threadPool.coroutineContext(ThreadPool.Names.MANAGEMENT)) {
            listener.completeWith {
                val injectedUser = SecurityContext.fromSecurityThreadContext(threadPool.threadContext)
                val replicationMetadata = clusterService.state().metadata.custom(ReplicationMetadata.NAME)
                        ?: ReplicationMetadata.EMPTY
                if (request.action == UpdateAutoFollowPatternRequest.Action.REMOVE) {
                    // Stopping the tasks and removing the context information from the cluster state
                    replicationMetadata.removePattern(request.connection, request.patternName).also {
                        val shouldStop = it.autoFollowPatterns[request.connection]?.get(request.patternName) == null
                        if (shouldStop) stopAutoFollowTask(request.connection, request.patternName)
                    }
                }

                val response: AcknowledgedResponse = clusterService.waitForClusterStateUpdate("update autofollow patterns") { l ->
                    UpdateAutoFollowPattern(request, threadPool, injectedUser, l)
                }

                if(!response.isAcknowledged) {
                    throw OpenSearchException(AUTOFOLLOW_EXCEPTION_GENERIC_STRING)
                }

                if (request.action == UpdateAutoFollowPatternRequest.Action.ADD) {
                    // Should start the task if there were no follow patterns before adding this
                    val shouldStart = replicationMetadata.autoFollowPatterns[request.connection]?.get(request.patternName) == null
                    if (shouldStart) startAutoFollowTask(request.connection, request.patternName)
                }
                response
            }
        }
    }

    override fun checkBlock(request: UpdateAutoFollowPatternRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    private suspend fun startAutoFollowTask(clusterAlias: String, patternName: String) {
        try {
            val response = persistentTasksService.startTask("autofollow:$clusterAlias:$patternName",
                    AutoFollowExecutor.TASK_NAME,
                    AutoFollowParams(clusterAlias, patternName))
            if (!response.isAssigned) {
                log.warn("""Failed to assign auto follow task for cluster $clusterAlias:$patternName to any node. Check if any
                    |cluster blocks are active.""".trimMargin())
            }
        } catch(e: ResourceAlreadyExistsException) {
            // Log and bail as task is already running
            log.warn("Task already started for '$clusterAlias:$patternName'", e)
        } catch (e: Exception) {
            log.error("Failed to start auto follow task for cluster '$clusterAlias:$patternName'", e)
            throw OpenSearchException(AUTOFOLLOW_EXCEPTION_GENERIC_STRING)
        }
    }

    private suspend fun stopAutoFollowTask(clusterAlias: String, patternName: String) {
        try {
            persistentTasksService.removeTask("autofollow:$clusterAlias:$patternName")
        } catch(e: ResourceNotFoundException) {
            // Log warn as the task is already removed
            log.warn("Task already stopped for '$clusterAlias:$patternName'", e)
        } catch (e: Exception) {
            log.error("Failed to stop auto follow task for cluster '$clusterAlias:$patternName'", e)
            throw OpenSearchException(AUTOFOLLOW_EXCEPTION_GENERIC_STRING)
        }
    }
}
