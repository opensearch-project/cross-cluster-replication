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

package com.amazon.elasticsearch.replication.action.autofollow

import com.amazon.elasticsearch.replication.ReplicationException
import com.amazon.elasticsearch.replication.metadata.ReplicationMetadataManager
import com.amazon.elasticsearch.replication.metadata.ReplicationOverallState
import com.amazon.elasticsearch.replication.task.autofollow.AutoFollowExecutor
import com.amazon.elasticsearch.replication.task.autofollow.AutoFollowParams
import com.amazon.elasticsearch.replication.util.SecurityContext
import com.amazon.elasticsearch.replication.util.completeWith
import com.amazon.elasticsearch.replication.util.coroutineContext
import com.amazon.elasticsearch.replication.util.persistentTasksService
import com.amazon.elasticsearch.replication.util.removeTask
import com.amazon.elasticsearch.replication.util.startTask
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.ResourceAlreadyExistsException
import org.elasticsearch.ResourceNotFoundException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.support.master.TransportMasterNodeAction
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.block.ClusterBlockLevel
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService

class TransportUpdateAutoFollowPatternAction @Inject constructor(
    transportService: TransportService, clusterService: ClusterService, threadPool: ThreadPool,
    actionFilters: ActionFilters, indexNameExpressionResolver: IndexNameExpressionResolver,
    private val client: NodeClient, private val metadataManager: ReplicationMetadataManager) : TransportMasterNodeAction<UpdateAutoFollowPatternRequest, AcknowledgedResponse>(
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
                val user = SecurityContext.fromSecurityThreadContext(threadPool.threadContext)
                if (request.action == UpdateAutoFollowPatternRequest.Action.REMOVE) {
                    // Stopping the tasks and removing the context information from the cluster state
                    stopAutoFollowTask(request.connection, request.patternName)
                    metadataManager.deleteAutofollowMetadata(request.patternName, request.connection)
                }

                if (request.action == UpdateAutoFollowPatternRequest.Action.ADD) {
                    // Should start the task if there were no follow patterns before adding this
                    if(request.pattern == null) {
                        throw ReplicationException("Failed to update empty autofollow pattern")
                    }
                    metadataManager.addAutofollowMetadata(request.patternName, request.connection, request.pattern,
                            ReplicationOverallState.RUNNING, user, user?.roles?.getOrNull(0), user?.roles?.getOrNull(0))
                    startAutoFollowTask(request.connection, request.patternName)
                }
                AcknowledgedResponse(true)
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
            throw ElasticsearchException(AUTOFOLLOW_EXCEPTION_GENERIC_STRING)
        }
    }

    private suspend fun stopAutoFollowTask(clusterAlias: String, patternName: String) {
        try {
            persistentTasksService.removeTask("autofollow:$clusterAlias:$patternName")
        } catch(e: ResourceNotFoundException) {
            // Log warn as the task is already removed
            log.warn("Task already stopped for '$clusterAlias:$patternName'", e)
            throw ElasticsearchException(AUTOFOLLOW_EXCEPTION_GENERIC_STRING)
        } catch (e: Exception) {
            log.error("Failed to stop auto follow task for cluster '$clusterAlias:$patternName'", e)
            throw ElasticsearchException(AUTOFOLLOW_EXCEPTION_GENERIC_STRING)
        }
    }
}
