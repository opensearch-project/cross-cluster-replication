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

import com.amazon.elasticsearch.replication.action.replicationstatedetails.UpdateReplicationStateDetailsRequest
import com.amazon.elasticsearch.replication.metadata.state.ReplicationStateMetadata
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.cluster.AckedClusterStateUpdateTask
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.ClusterStateTaskExecutor
import org.elasticsearch.cluster.ack.AckedRequest
import org.elasticsearch.cluster.metadata.Metadata

abstract class UpdateReplicationMetadata<T>(request: AckedRequest, listener: ActionListener<T>)
    : AckedClusterStateUpdateTask<T>(request, listener) {

    override fun execute(currentState: ClusterState): ClusterState {
        val currentMetadata = currentState.metadata().custom(ReplicationStateMetadata.NAME) ?: ReplicationStateMetadata.EMPTY
        val newMetadata = updateMetadata(currentMetadata)
        return if (currentMetadata == newMetadata) {
            currentState // no change
        } else {
            val mdBuilder = Metadata.builder(currentState.metadata)
                .putCustom(ReplicationStateMetadata.NAME, newMetadata)
            ClusterState.Builder(currentState).metadata(mdBuilder).build()
        }
    }

    abstract fun updateMetadata(currentStateMetadata: ReplicationStateMetadata): ReplicationStateMetadata
}

class UpdateReplicationStateDetailsTaskExecutor private constructor()
    : ClusterStateTaskExecutor<UpdateReplicationStateDetailsRequest> {

    companion object {
        private val log = LogManager.getLogger(UpdateReplicationStateDetailsTaskExecutor::class.java)
        val INSTANCE = UpdateReplicationStateDetailsTaskExecutor()
    }

    override fun execute(currentState: ClusterState, tasks: List<UpdateReplicationStateDetailsRequest>)
            : ClusterStateTaskExecutor.ClusterTasksResult<UpdateReplicationStateDetailsRequest> {
        log.debug("Executing replication state update for $tasks")
        return getClusterStateUpdateTaskResult(tasks, currentState)
    }

    private fun getClusterStateUpdateTaskResult(requests: List<UpdateReplicationStateDetailsRequest>,
                                                currentState: ClusterState)
            : ClusterStateTaskExecutor.ClusterTasksResult<UpdateReplicationStateDetailsRequest> {
        val currentMetadata = currentState.metadata().custom(ReplicationStateMetadata.NAME) ?: ReplicationStateMetadata.EMPTY
        var updatedMetadata = currentMetadata
        // compute metadata update for the batched requests
        for(request in requests) {
            updatedMetadata = getUpdatedReplicationMetadata(request, updatedMetadata)
        }
        if (currentMetadata == updatedMetadata) {
            return getStateUpdateTaskResultForClusterState(requests, currentState) // no change
        } else {
            val mdBuilder = Metadata.builder(currentState.metadata)
                    .putCustom(ReplicationStateMetadata.NAME, updatedMetadata)
            val newClusterState = ClusterState.Builder(currentState).metadata(mdBuilder).build()
            return getStateUpdateTaskResultForClusterState(requests, newClusterState)
        }
    }

    private fun getStateUpdateTaskResultForClusterState(requests: List<UpdateReplicationStateDetailsRequest>,
                                                        clusterState: ClusterState)
            : ClusterStateTaskExecutor.ClusterTasksResult<UpdateReplicationStateDetailsRequest> {
        return ClusterStateTaskExecutor.ClusterTasksResult.builder<UpdateReplicationStateDetailsRequest>()
                .successes(requests).build(clusterState)
    }

    private fun getUpdatedReplicationMetadata(request: UpdateReplicationStateDetailsRequest,
                                              currentStateMetadata: ReplicationStateMetadata)
            : ReplicationStateMetadata {
        if (request.updateType == UpdateReplicationStateDetailsRequest.UpdateType.ADD)
            return currentStateMetadata.addReplicationStateParams(request.followIndexName,
                request.replicationStateParams)
        return currentStateMetadata.removeReplicationStateParams(request.followIndexName)
    }
}
