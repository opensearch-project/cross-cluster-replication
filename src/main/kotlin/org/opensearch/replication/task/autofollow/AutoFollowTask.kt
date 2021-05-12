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

package org.opensearch.replication.task.autofollow

import org.opensearch.replication.action.index.ReplicateIndexAction
import org.opensearch.replication.action.index.ReplicateIndexRequest
import org.opensearch.replication.metadata.ReplicationMetadata
import org.opensearch.replication.task.CrossClusterReplicationTask
import org.opensearch.replication.task.ReplicationState
import org.opensearch.replication.util.suspending
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import org.opensearch.action.admin.indices.get.GetIndexRequest
import org.opensearch.action.support.IndicesOptions
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.logging.Loggers
import org.opensearch.common.unit.TimeValue
import org.opensearch.persistent.PersistentTaskState
import org.opensearch.replication.util.suspendExecute
import org.opensearch.tasks.TaskId
import org.opensearch.threadpool.ThreadPool

class AutoFollowTask(id: Long, type: String, action: String, description: String, parentTask: TaskId,
                     headers: Map<String, String>,
                     executor: String,
                     clusterService: ClusterService,
                     threadPool: ThreadPool,
                     client: Client,
                     val params: AutoFollowParams) :
    CrossClusterReplicationTask(id, type, action, description, parentTask, headers,
                                executor, clusterService, threadPool, client) {

    override val remoteCluster = params.remoteCluster
    val patternName = params.patternName
    override val followerIndexName: String = ReplicationMetadata.AUTOFOLLOW_SECURITY_CONTEXT_PATTERN_PREFIX +
            params.patternName //Special case for auto follow
    override val log = Loggers.getLogger(javaClass, remoteCluster)
    private var trackingIndicesOnTheCluster = setOf<String>()

    companion object {
        //TODO: Convert to setting
        val AUTO_FOLLOW_CHECK_DELAY = TimeValue.timeValueSeconds(30)!!
    }

    override suspend fun execute(initialState: PersistentTaskState?) {
        while (scope.isActive) {
            autoFollow()
            delay(AUTO_FOLLOW_CHECK_DELAY.millis)
        }
    }

    private suspend fun autoFollow() {
        log.debug("Checking $remoteCluster under pattern name $patternName for new indices to auto follow")
        val replicationMetadata = clusterService.state().metadata.custom(ReplicationMetadata.NAME) ?: ReplicationMetadata.EMPTY
        val entry = replicationMetadata.autoFollowPatterns[remoteCluster]?.get(patternName)
        if (entry?.pattern == null) {
            log.debug("No auto follow patterns setup for cluster $remoteCluster with pattern name $followerIndexName")
            return
        }

        // Fetch remote indices matching auto follow pattern
        val remoteClient = client.getRemoteClusterClient(remoteCluster)
        val indexReq = GetIndexRequest().features(*emptyArray())
                .indices(entry.pattern)
                .indicesOptions(IndicesOptions.lenientExpandOpen())
        val response = suspending(remoteClient.admin().indices()::getIndex)(indexReq)
        var remoteIndices = response.indices.asIterable()

        val replicatedRemoteIndices = replicationMetadata.replicatedIndices
            .getOrDefault(remoteCluster, emptyMap()).values
        remoteIndices = remoteIndices.minus(replicatedRemoteIndices)

        var currentIndices = clusterService.state().metadata().concreteAllIndices.asIterable() // All indices - open and closed on the cluster
        if(remoteIndices.intersect(currentIndices).isNotEmpty()) {
            // Log this once when we see any update on indices on the follower cluster to prevent log flood
            if(currentIndices.toSet() != trackingIndicesOnTheCluster) {
                log.info("Cannot initiate replication for the following indices from remote ($remoteCluster) as indices with " +
                        "same name already exists on the cluster ${remoteIndices.intersect(currentIndices)}")
                trackingIndicesOnTheCluster = currentIndices.toSet()
            }
        }
        remoteIndices = remoteIndices.minus(currentIndices)

        for (newRemoteIndex in remoteIndices) {
            startReplication(newRemoteIndex)
        }
    }

    private suspend fun startReplication(remoteIndex: String) {
        if (clusterService.state().metadata().hasIndex(remoteIndex)) {
            log.info("""Cannot replicate $remoteCluster:$remoteIndex as an index with the same name already 
                        |exists.""".trimMargin())
            return
        }

        try {
            log.info("Auto follow starting replication from ${remoteCluster}:$remoteIndex -> $remoteIndex")
            val request = ReplicateIndexRequest(remoteIndex, remoteCluster, remoteIndex)
            val response = client.suspendExecute(ReplicateIndexAction.INSTANCE, request)
            if (!response.isAcknowledged) {
                log.warn("Failed to auto follow remote index $remoteIndex")
            }
        } catch (e: Exception) {
            log.warn("Failed to start replication for $remoteCluster:$remoteIndex -> $remoteIndex.", e)
        }
    }

    override fun toString(): String {
        return "AutoFollowTask(from=${remoteCluster} with pattern=${params.patternName})"
    }

    override fun replicationTaskResponse(): CrossClusterReplicationTaskResponse {
        return CrossClusterReplicationTaskResponse(ReplicationState.COMPLETED.name)
    }
}