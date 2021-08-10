package org.opensearch.replication.action.autofollow

import org.opensearch.replication.ReplicationException
import org.opensearch.replication.action.index.ReplicateIndexRequest
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.metadata.ReplicationOverallState
import org.opensearch.replication.task.autofollow.AutoFollowExecutor
import org.opensearch.replication.task.autofollow.AutoFollowParams
import org.opensearch.replication.util.completeWith
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.persistentTasksService
import org.opensearch.replication.util.removeTask
import org.opensearch.replication.util.startTask
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
import org.opensearch.common.settings.IndexScopedSettings
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService

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
                        throw org.opensearch.replication.ReplicationException("Failed to update empty autofollow pattern")
                    }
                    // Pattern is same for leader and follower
                    val followerClusterRole = request.assumeRoles?.get(ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE)
                    val leaderClusterRole = request.assumeRoles?.get(ReplicateIndexRequest.LEADER_CLUSTER_ROLE)

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