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

package org.opensearch.replication.action.index

import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.metadata.ReplicationOverallState
import org.opensearch.replication.task.ReplicationState
import org.opensearch.replication.task.index.IndexReplicationExecutor
import org.opensearch.replication.task.index.IndexReplicationParams
import org.opensearch.replication.task.index.IndexReplicationState
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.startTask
import org.opensearch.replication.util.suspending
import org.opensearch.replication.util.waitForTaskCondition
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.core.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction
import org.opensearch.transport.client.node.NodeClient
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.common.settings.IndexScopedSettings
import org.opensearch.index.IndexNotFoundException
import org.opensearch.persistent.PersistentTasksService
import org.opensearch.replication.ReplicationPlugin
import org.opensearch.replication.util.stackTraceToString
import org.opensearch.repositories.RepositoriesService
import org.opensearch.core.rest.RestStatus
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import java.io.IOException

class TransportReplicateIndexClusterManagerNodeAction @Inject constructor(transportService: TransportService,
                                                                          clusterService: ClusterService,
                                                                          threadPool: ThreadPool,
                                                                          actionFilters: ActionFilters,
                                                                          indexNameExpressionResolver: IndexNameExpressionResolver,
                                                                          val indexScopedSettings: IndexScopedSettings,
                                                                          private val persistentTasksService: PersistentTasksService,
                                                                          private val nodeClient : NodeClient,
                                                                          private val repositoryService: RepositoriesService,
                                                                          private val replicationMetadataManager: ReplicationMetadataManager) :
    TransportClusterManagerNodeAction<ReplicateIndexClusterManagerNodeRequest, AcknowledgedResponse>(ReplicateIndexClusterManagerNodeAction.NAME,
                transportService, clusterService, threadPool, actionFilters, ::ReplicateIndexClusterManagerNodeRequest, indexNameExpressionResolver),
        CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportReplicateIndexClusterManagerNodeAction::class.java)
    }

    override fun executor(): String {
        return ThreadPool.Names.SAME
    }

    @Throws(IOException::class)
    override fun read(input: StreamInput): ReplicateIndexResponse {
        return ReplicateIndexResponse(input)
    }

    @Throws(Exception::class)
    override fun clusterManagerOperation(request: ReplicateIndexClusterManagerNodeRequest, state: ClusterState,
                                 listener: ActionListener<AcknowledgedResponse>) {
        val replicateIndexReq = request.replicateIndexReq
        val user = request.user
        log.trace("Triggering relevant tasks to start replication for " +
                "${replicateIndexReq.leaderAlias}:${replicateIndexReq.leaderIndex} -> ${replicateIndexReq.followerIndex}")

        // For now this returns a response after creating the follower index and starting the replication tasks
        // for each shard. If that takes too long we can start the task asynchronously and return the response first.
        launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            try {
                if(clusterService.clusterSettings.get(ReplicationPlugin.REPLICATION_FOLLOWER_BLOCK_START)) {
                    log.debug("Replication cannot be started as " +
                            "start block(${ReplicationPlugin.REPLICATION_FOLLOWER_BLOCK_START}) is set")
                    throw OpenSearchStatusException("[FORBIDDEN] Replication START block is set", RestStatus.FORBIDDEN)
                }

                log.debug("Making request to get metadata of ${replicateIndexReq.leaderIndex} index on remote cluster")
                val remoteMetadata = getRemoteIndexMetadata(replicateIndexReq.leaderAlias, replicateIndexReq.leaderIndex)
                log.debug("Response returned of the request made to get metadata of ${replicateIndexReq.leaderIndex} index on remote cluster")

                if (state.routingTable.hasIndex(replicateIndexReq.followerIndex)) {
                    throw IllegalArgumentException("Cant use same index again for replication. " +
                    "Delete the index:${replicateIndexReq.followerIndex}")
                }

                indexScopedSettings.validate(replicateIndexReq.settings,
                        false,
                        false)

                val params = IndexReplicationParams(replicateIndexReq.leaderAlias, remoteMetadata.index, replicateIndexReq.followerIndex)

                replicationMetadataManager.addIndexReplicationMetadata(replicateIndexReq.followerIndex,
                        replicateIndexReq.leaderAlias, replicateIndexReq.leaderIndex,
                        ReplicationOverallState.RUNNING, user, replicateIndexReq.useRoles?.getOrDefault(ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE, null),
                        replicateIndexReq.useRoles?.getOrDefault(ReplicateIndexRequest.LEADER_CLUSTER_ROLE, null), replicateIndexReq.settings)

                log.debug("Starting index replication task in persistent task service with name: replication:index:${replicateIndexReq.followerIndex}")
                val task = persistentTasksService.startTask("replication:index:${replicateIndexReq.followerIndex}",
                        IndexReplicationExecutor.TASK_NAME, params)

                if (!task.isAssigned) {
                    log.error("Failed to assign task")
                    listener.onResponse(ReplicateIndexResponse(false))
                }

                log.debug("Waiting for persistent task to move to following state")
                // Now wait for the replication to start and the follower index to get created before returning
                persistentTasksService.waitForTaskCondition(task.id, replicateIndexReq.timeout()) { t ->
                    val replicationState = (t.state as IndexReplicationState?)?.state
                    replicationState == ReplicationState.FOLLOWING ||
                            (!replicateIndexReq.waitForRestore && replicationState == ReplicationState.RESTORING)
                }
                log.debug("Persistent task is moved to following replication state")
                listener.onResponse(AcknowledgedResponse(true))
            } catch (e: Exception) {
                log.error("Failed to trigger replication for ${replicateIndexReq.followerIndex} - ${e.stackTraceToString()}")
                listener.onFailure(e)
            }
        }
    }

    private suspend fun getRemoteIndexMetadata(leaderAlias: String, leaderIndex: String): IndexMetadata {
        val remoteClusterClient = nodeClient.getRemoteClusterClient(leaderAlias)
        val clusterStateRequest = remoteClusterClient.admin().cluster().prepareState()
                .clear()
                .setIndices(leaderIndex)
                .setMetadata(true)
                .setIndicesOptions(IndicesOptions.strictSingleIndexNoExpandForbidClosed())
                .request()
        val remoteState = remoteClusterClient.suspending(remoteClusterClient.admin().cluster()::state,
                injectSecurityContext = true, defaultContext = true)(clusterStateRequest).state
        return remoteState.metadata.index(leaderIndex) ?: throw IndexNotFoundException("${leaderAlias}:${leaderIndex}")
    }

    override fun checkBlock(request: ReplicateIndexClusterManagerNodeRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks.globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }
}
