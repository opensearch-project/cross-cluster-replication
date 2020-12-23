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

package com.amazon.elasticsearch.replication.action.index

import com.amazon.elasticsearch.replication.action.replicationstatedetails.UpdateReplicationStateDetailsRequest
import com.amazon.elasticsearch.replication.metadata.REPLICATION_OVERALL_STATE_KEY
import com.amazon.elasticsearch.replication.metadata.REPLICATION_OVERALL_STATE_RUNNING_VALUE
import com.amazon.elasticsearch.replication.metadata.UpdateReplicatedIndices
import com.amazon.elasticsearch.replication.metadata.UpdateReplicationStateDetailsTaskExecutor
import com.amazon.elasticsearch.replication.task.ReplicationState
import com.amazon.elasticsearch.replication.task.index.IndexReplicationExecutor
import com.amazon.elasticsearch.replication.task.index.IndexReplicationParams
import com.amazon.elasticsearch.replication.task.index.IndexReplicationState
import com.amazon.elasticsearch.replication.util.coroutineContext
import com.amazon.elasticsearch.replication.util.startTask
import com.amazon.elasticsearch.replication.util.submitClusterStateUpdateTask
import com.amazon.elasticsearch.replication.util.suspending
import com.amazon.elasticsearch.replication.util.waitForClusterStateUpdate
import com.amazon.elasticsearch.replication.util.waitForTaskCondition
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.master.AcknowledgedRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.support.master.TransportMasterNodeAction
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.ClusterStateTaskExecutor
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.block.ClusterBlockLevel
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.persistent.PersistentTasksService
import org.elasticsearch.repositories.RepositoriesService
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService
import java.io.IOException

class TransportReplicateIndexMasterNodeAction @Inject constructor(transportService: TransportService,
                                                                  clusterService: ClusterService,
                                                                  threadPool: ThreadPool,
                                                                  actionFilters: ActionFilters,
                                                                  indexNameExpressionResolver: IndexNameExpressionResolver,
                                                                  private val persistentTasksService: PersistentTasksService,
                                                                  private val nodeClient : NodeClient,
                                                                  private val repositoryService: RepositoriesService) :
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
                "${replicateIndexReq.remoteCluster}:${replicateIndexReq.remoteIndex} -> ${replicateIndexReq.followerIndex}")

        // For now this returns a response after creating the follower index and starting the replication tasks
        // for each shard. If that takes too long we can start the task asynchronously and return the response first.
        launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            try {
                val remoteMetadata = getRemoteIndexMetadata(replicateIndexReq.remoteCluster, replicateIndexReq.remoteIndex)
                val params = IndexReplicationParams(replicateIndexReq.remoteCluster, remoteMetadata.index, replicateIndexReq.followerIndex)
                updateReplicationStateToStarted(replicateIndexReq.followerIndex)

                val response : ReplicateIndexResponse =
                        clusterService.waitForClusterStateUpdate("updating replicated indices") { l ->
                            UpdateReplicatedIndices(replicateIndexReq, user, l)
                        }

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

                listener.onResponse(response)
            } catch (e: Exception) {
                log.error("Failed to trigger replication for ${replicateIndexReq.followerIndex} - $e")
                listener.onFailure(e)
            }
        }
    }

    private suspend fun updateReplicationStateToStarted(indexName: String) {
        val replicationStateParamMap = HashMap<String, String>()
        replicationStateParamMap[REPLICATION_OVERALL_STATE_KEY] = REPLICATION_OVERALL_STATE_RUNNING_VALUE
        val updateReplicationStateDetailsRequest = UpdateReplicationStateDetailsRequest(indexName, replicationStateParamMap,
                UpdateReplicationStateDetailsRequest.UpdateType.ADD)
        submitClusterStateUpdateTask(updateReplicationStateDetailsRequest, UpdateReplicationStateDetailsTaskExecutor.INSTANCE
                as ClusterStateTaskExecutor<AcknowledgedRequest<UpdateReplicationStateDetailsRequest>>,
                clusterService,
                "remove-replication-state-params")
    }

    private suspend fun getRemoteIndexMetadata(remoteCluster: String, remoteIndex: String): IndexMetadata {
        val remoteClusterClient = nodeClient.getRemoteClusterClient(remoteCluster).admin().cluster()
        val clusterStateRequest = remoteClusterClient.prepareState()
                .clear()
                .setIndices(remoteIndex)
                .setMetadata(true)
                .setIndicesOptions(IndicesOptions.strictSingleIndexNoExpandForbidClosed())
                .request()
        val remoteState = suspending(remoteClusterClient::state)(clusterStateRequest).state
        return remoteState.metadata.index(remoteIndex) ?: throw IndexNotFoundException("${remoteCluster}:${remoteIndex}")
    }

    override fun checkBlock(request: ReplicateIndexMasterNodeRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks.globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }
}
