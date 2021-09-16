package com.amazon.elasticsearch.replication.action.autofollow

import com.amazon.elasticsearch.replication.ReplicationException
import com.amazon.elasticsearch.replication.action.index.ReplicateIndexRequest
import com.amazon.elasticsearch.replication.metadata.ReplicationMetadataManager
import com.amazon.elasticsearch.replication.metadata.ReplicationOverallState
import com.amazon.elasticsearch.replication.task.autofollow.AutoFollowExecutor
import com.amazon.elasticsearch.replication.task.autofollow.AutoFollowParams
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
import org.elasticsearch.common.settings.IndexScopedSettings
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService

class TransportAutoFollowMasterNodeAction @Inject constructor(transportService: TransportService, clusterService: ClusterService, threadPool: ThreadPool,
                                                              actionFilters: ActionFilters, indexNameExpressionResolver: IndexNameExpressionResolver,
                                                              private val client: NodeClient, private val metadataManager: ReplicationMetadataManager,
                                                              val indexScopedSettings: IndexScopedSettings) :
        TransportMasterNodeAction<AutoFollowMasterNodeRequest, AcknowledgedResponse>(
        AutoFollowMasterNodeAction.NAME, true, transportService, clusterService, threadPool, actionFilters,
        ::AutoFollowMasterNodeRequest, indexNameExpressionResolver), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportAutoFollowMasterNodeAction::class.java)
        const val AUTOFOLLOW_EXCEPTION_GENERIC_STRING = "Failed to update autofollow pattern"
    }

    override fun checkBlock(request: AutoFollowMasterNodeRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    override fun masterOperation(masterNodeReq: AutoFollowMasterNodeRequest, state: ClusterState, listener: ActionListener<AcknowledgedResponse>) {
        val request = masterNodeReq.autofollowReq
        var user = masterNodeReq.user
        launch(threadPool.coroutineContext()) {
            listener.completeWith {
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
                    // Pattern is same for leader and follower
                    val followerClusterRole = request.useRoles?.get(ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE)
                    val leaderClusterRole = request.useRoles?.get(ReplicateIndexRequest.LEADER_CLUSTER_ROLE)

                    indexScopedSettings.validate(request.settings,
                            false,
                            false)

                    metadataManager.addAutofollowMetadata(request.patternName, request.connection, request.pattern!!,
                            ReplicationOverallState.RUNNING, user, followerClusterRole, leaderClusterRole, request.settings)
                    startAutoFollowTask(request.connection, request.patternName)
                }
                AcknowledgedResponse(true)
            }
        }
    }

    override fun executor(): String = ThreadPool.Names.SAME

    override fun read(inp: StreamInput) = AcknowledgedResponse(inp)

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
        } catch (e: Exception) {
           log.error("Failed to stop auto follow task for cluster '$clusterAlias:$patternName'", e)
            throw ElasticsearchException(AUTOFOLLOW_EXCEPTION_GENERIC_STRING)
        }
    }
}