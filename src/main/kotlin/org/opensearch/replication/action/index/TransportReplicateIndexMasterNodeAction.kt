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
import org.opensearch.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.action.support.master.TransportMasterNodeAction
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.settings.IndexScopedSettings
import org.opensearch.index.IndexNotFoundException
import org.opensearch.persistent.PersistentTasksService
import org.opensearch.repositories.RepositoriesService
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import java.io.IOException

class TransportReplicateIndexMasterNodeAction @Inject constructor(transportService: TransportService,
                                                                  clusterService: ClusterService,
                                                                  threadPool: ThreadPool,
                                                                  actionFilters: ActionFilters,
                                                                  indexNameExpressionResolver: IndexNameExpressionResolver,
                                                                  val indexScopedSettings: IndexScopedSettings,
                                                                  private val persistentTasksService: PersistentTasksService,
                                                                  private val nodeClient : NodeClient,
                                                                  private val repositoryService: RepositoriesService,
                                                                  private val replicationMetadataManager: ReplicationMetadataManager) :
        TransportMasterNodeAction<ReplicateIndexMasterNodeRequest, AcknowledgedResponse>(ReplicateIndexMasterNodeAction.NAME,
                transportService, clusterService, threadPool, actionFilters, ::ReplicateIndexMasterNodeRequest, indexNameExpressionResolver),
        CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportReplicateIndexMasterNodeAction::class.java)
    }

    override fun executor(): String {
        return ThreadPool.Names.SAME
    }

    @Throws(IOException::class)
    override fun read(input: StreamInput): ReplicateIndexResponse {
        return ReplicateIndexResponse(input)
    }

    @Throws(Exception::class)
    override fun masterOperation(request: ReplicateIndexMasterNodeRequest, state: ClusterState,
                                 listener: ActionListener<AcknowledgedResponse>) {
        val replicateIndexReq = request.replicateIndexReq
        val user = request.user
        log.trace("Triggering relevant tasks to start replication for " +
                "${replicateIndexReq.leaderAlias}:${replicateIndexReq.leaderIndex} -> ${replicateIndexReq.followerIndex}")

        // For now this returns a response after creating the follower index and starting the replication tasks
        // for each shard. If that takes too long we can start the task asynchronously and return the response first.
        launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            try {
                val remoteMetadata = getRemoteIndexMetadata(replicateIndexReq.leaderAlias, replicateIndexReq.leaderIndex)

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
                        ReplicationOverallState.RUNNING, user, replicateIndexReq.assumeRoles?.getOrDefault(ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE, null),
                        replicateIndexReq.assumeRoles?.getOrDefault(ReplicateIndexRequest.LEADER_CLUSTER_ROLE, null), replicateIndexReq.settings)

                val task = persistentTasksService.startTask("replication:index:${replicateIndexReq.followerIndex}",
                        IndexReplicationExecutor.TASK_NAME, params)

                if (!task.isAssigned) {
                    log.error("Failed to assign task")
                    listener.onResponse(ReplicateIndexResponse(false))
                }

                // Now wait for the replication to start and the follower index to get created before returning
                persistentTasksService.waitForTaskCondition(task.id, replicateIndexReq.timeout()) { t ->
                    val replicationState = (t.state as IndexReplicationState?)?.state
                    replicationState == ReplicationState.FOLLOWING ||
                            (!replicateIndexReq.waitForRestore && replicationState == ReplicationState.RESTORING)
                }

                listener.onResponse(AcknowledgedResponse(true))
            } catch (e: Exception) {
                log.error("Failed to trigger replication for ${replicateIndexReq.followerIndex} - $e")
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

    override fun checkBlock(request: ReplicateIndexMasterNodeRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks.globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }
}
