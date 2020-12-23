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

package com.amazon.elasticsearch.replication.metadata

import com.amazon.elasticsearch.replication.action.autofollow.UpdateAutoFollowPatternRequest
import com.amazon.elasticsearch.replication.action.index.ReplicateIndexRequest
import com.amazon.elasticsearch.replication.action.index.ReplicateIndexResponse
import com.amazon.elasticsearch.replication.action.replicationstatedetails.UpdateReplicationStateDetailsRequest
import com.amazon.elasticsearch.replication.action.stop.StopIndexReplicationRequest
import com.amazon.elasticsearch.replication.task.autofollow.AutoFollowExecutor
import com.amazon.elasticsearch.replication.task.autofollow.AutoFollowParams
import com.amazon.elasticsearch.replication.util.coroutineContext
import com.amazon.elasticsearch.replication.util.persistentTasksService
import com.amazon.elasticsearch.replication.util.removeTask
import com.amazon.elasticsearch.replication.util.startTask
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.master.AcknowledgedRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.support.master.MasterNodeRequest
import org.elasticsearch.cluster.AckedClusterStateUpdateTask
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.ClusterStateTaskExecutor
import org.elasticsearch.cluster.ack.AckedRequest
import org.elasticsearch.cluster.metadata.Metadata
import org.elasticsearch.threadpool.ThreadPool

abstract class UpdateReplicationMetadata<T>(request: AckedRequest, listener: ActionListener<T>)
    : AckedClusterStateUpdateTask<T>(request, listener) {

    override fun execute(currentState: ClusterState): ClusterState {
        val currentMetadata = currentState.metadata().custom(ReplicationMetadata.NAME) ?: ReplicationMetadata.EMPTY
        val newMetadata = updateMetadata(currentMetadata)
        return if (currentMetadata == newMetadata) {
            currentState // no change
        } else {
            val mdBuilder = Metadata.builder(currentState.metadata)
                .putCustom(ReplicationMetadata.NAME, newMetadata)
            ClusterState.Builder(currentState).metadata(mdBuilder).build()
        }
    }

    abstract fun updateMetadata(currentMetadata: ReplicationMetadata): ReplicationMetadata
}

class UpdateAutoFollowPattern(val request: UpdateAutoFollowPatternRequest,
                              val threadPool: ThreadPool,
                              val injectedUser: String?,
                              listener: ActionListener<AcknowledgedResponse>)
    : UpdateReplicationMetadata<AcknowledgedResponse>(request, listener) {

    override fun updateMetadata(currentMetadata: ReplicationMetadata) : ReplicationMetadata {
        return when (request.action) {
            UpdateAutoFollowPatternRequest.Action.REMOVE -> {
                currentMetadata.removePattern(request.connection, request.patternName)
                        .removeSecurityContext(request.connection, ReplicationMetadata.AUTOFOLLOW_SECURITY_CONTEXT_PATTERN_PREFIX
                                + request.patternName)
            }

            UpdateAutoFollowPatternRequest.Action.ADD -> {
                val newPattern = AutoFollowPattern(request.patternName,
                                                   checkNotNull(request.pattern) { "null pattern" })
                currentMetadata.addPattern(request.connection, newPattern)
                        .addSecurityContext(request.connection, ReplicationMetadata.AUTOFOLLOW_SECURITY_CONTEXT_PATTERN_PREFIX
                                + request.patternName, injectedUser)
            }
        }
    }

    override fun newResponse(acknowledged: Boolean) = AcknowledgedResponse(acknowledged)
}

class UpdateReplicatedIndices<T : MasterNodeRequest<T>>(val request: AcknowledgedRequest<T>,
                                 val injectedUser: String?,
                                 listener: ActionListener<ReplicateIndexResponse>)
    : UpdateReplicationMetadata<ReplicateIndexResponse>(request, listener) {

    override fun updateMetadata(currentMetadata: ReplicationMetadata): ReplicationMetadata {
        if (request is ReplicateIndexRequest)
            return currentMetadata.addIndex(request.remoteCluster, request.followerIndex, request.remoteIndex)
                .addSecurityContext(request.remoteCluster, request.followerIndex, injectedUser)
        else if(request is StopIndexReplicationRequest) {
            val clusterAlias = currentMetadata.replicatedIndices.entries.firstOrNull {
                it.value.containsKey(request.indexName)}?.key
            clusterAlias?: throw IllegalStateException("Cant find cluster alias for follower index:${request.indexName}")
            return currentMetadata.removeIndex(clusterAlias, request.indexName)
                    .removeSecurityContext(clusterAlias, request.indexName)
        }
        throw IllegalArgumentException("Unrecognised request:$request")
    }

    override fun newResponse(acknowledged: Boolean): ReplicateIndexResponse = ReplicateIndexResponse(acknowledged)
}

class UpdateReplicationStateDetailsTaskExecutor private constructor()
    : ClusterStateTaskExecutor<UpdateReplicationStateDetailsRequest> {

    companion object {
        private val log = LogManager.getLogger(UpdateReplicationStateDetailsTaskExecutor::class.java)
        val INSTANCE = UpdateReplicationStateDetailsTaskExecutor()
    }

    override fun execute(currentState: ClusterState, tasks: List<UpdateReplicationStateDetailsRequest>)
            : ClusterStateTaskExecutor.ClusterTasksResult<UpdateReplicationStateDetailsRequest> {
        return getClusterStateUpdateTaskResult(tasks[0], currentState)
    }

    private fun getClusterStateUpdateTaskResult(request: UpdateReplicationStateDetailsRequest,
                                                currentState: ClusterState)
            : ClusterStateTaskExecutor.ClusterTasksResult<UpdateReplicationStateDetailsRequest> {
        val currentMetadata = currentState.metadata().custom(ReplicationMetadata.NAME) ?: ReplicationMetadata.EMPTY
        val newMetadata = getUpdatedReplicationMetadata(request, currentMetadata)
        if (currentMetadata == newMetadata) {
            return getStateUpdateTaskResultForClusterState(request, currentState) // no change
        } else {
            val mdBuilder = Metadata.builder(currentState.metadata)
                    .putCustom(ReplicationMetadata.NAME, newMetadata)
            val newClusterState = ClusterState.Builder(currentState).metadata(mdBuilder).build()
            return getStateUpdateTaskResultForClusterState(request, newClusterState)
        }
    }

    private fun getStateUpdateTaskResultForClusterState(request: UpdateReplicationStateDetailsRequest,
                                                        clusterState: ClusterState)
            : ClusterStateTaskExecutor.ClusterTasksResult<UpdateReplicationStateDetailsRequest> {
        return ClusterStateTaskExecutor.ClusterTasksResult.builder<UpdateReplicationStateDetailsRequest>()
                .success(request).build(clusterState)
    }

    private fun getUpdatedReplicationMetadata(request: UpdateReplicationStateDetailsRequest,
                                              currentMetadata: ReplicationMetadata)
            : ReplicationMetadata {
        if (request.updateType == UpdateReplicationStateDetailsRequest.UpdateType.ADD)
            return currentMetadata.addReplicationStateParams(request.followIndexName,
                request.replicationStateParams)
        return currentMetadata.removeReplicationStateParams(request.followIndexName)
    }
}
