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

package org.opensearch.replication.metadata

import org.opensearch.replication.action.replicationstatedetails.UpdateReplicationStateDetailsRequest
import org.opensearch.replication.metadata.state.ReplicationStateMetadata
import org.apache.logging.log4j.LogManager
import org.opensearch.core.action.ActionListener
import org.opensearch.cluster.AckedClusterStateUpdateTask
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.ClusterStateTaskExecutor
import org.opensearch.cluster.ack.AckedRequest
import org.opensearch.cluster.metadata.Metadata

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
