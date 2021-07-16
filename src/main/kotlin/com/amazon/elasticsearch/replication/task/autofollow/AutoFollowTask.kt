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

package com.amazon.elasticsearch.replication.task.autofollow

import com.amazon.elasticsearch.replication.ReplicationException
import com.amazon.elasticsearch.replication.ReplicationSettings
import com.amazon.elasticsearch.replication.action.index.ReplicateIndexAction
import com.amazon.elasticsearch.replication.action.index.ReplicateIndexRequest
import com.amazon.elasticsearch.replication.metadata.ReplicationMetadataManager
import com.amazon.elasticsearch.replication.task.CrossClusterReplicationTask
import com.amazon.elasticsearch.replication.task.ReplicationState
import com.amazon.elasticsearch.replication.util.stackTraceToString
import com.amazon.elasticsearch.replication.util.suspendExecute
import com.amazon.elasticsearch.replication.util.suspending
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import org.elasticsearch.ElasticsearchSecurityException
import org.elasticsearch.action.admin.indices.get.GetIndexRequest
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.persistent.PersistentTaskState
import org.elasticsearch.tasks.TaskId
import org.elasticsearch.threadpool.Scheduler
import org.elasticsearch.threadpool.ThreadPool
import java.util.concurrent.ConcurrentSkipListSet

class AutoFollowTask(id: Long, type: String, action: String, description: String, parentTask: TaskId,
                     headers: Map<String, String>,
                     executor: String,
                     clusterService: ClusterService,
                     threadPool: ThreadPool,
                     client: Client,
                     replicationMetadataManager: ReplicationMetadataManager,
                     val params: AutoFollowParams,
                     replicationSettings: ReplicationSettings) :
    CrossClusterReplicationTask(id, type, action, description, parentTask, headers,
                                executor, clusterService, threadPool, client, replicationMetadataManager, replicationSettings) {

    override val remoteCluster = params.remoteCluster
    val patternName = params.patternName
    override val followerIndexName: String = params.patternName //Special case for auto follow
    override val log = Loggers.getLogger(javaClass, remoteCluster)
    private var trackingIndicesOnTheCluster = setOf<String>()
    private var failedIndices = ConcurrentSkipListSet<String>() // Failed indices for replication from this autofollow task
    private var retryScheduler: Scheduler.ScheduledCancellable? = null
    private var failureCount = 0

    override suspend fun execute(initialState: PersistentTaskState?) {
        while (scope.isActive) {
            addRetryScheduler()
            autoFollow()
            delay(replicationSettings.autofollowFetchPollDuration.millis)
        }
    }

    private fun addRetryScheduler() {
        if(retryScheduler != null && !retryScheduler!!.isCancelled) {
            return
        }
        retryScheduler = try {
            threadPool.schedule({ failedIndices.clear() }, replicationSettings.autofollowRetryPollDuration, ThreadPool.Names.GENERIC)
        } catch (e: Exception) {
            log.error("Error scheduling retry on failed autofollow indices ${e.stackTraceToString()}")
            null
        }
    }

    override suspend fun cleanup() {
        retryScheduler?.cancel()
    }

    private suspend fun autoFollow() {
        log.debug("Checking $remoteCluster under pattern name $patternName for new indices to auto follow")
        val entry = replicationMetadata.leaderContext.resource

        // Fetch remote indices matching auto follow pattern
        var remoteIndices = Iterable { emptyArray<String>().iterator() }
        try {
            val remoteClient = client.getRemoteClusterClient(remoteCluster)
            val indexReq = GetIndexRequest().features(*emptyArray())
                    .indices(entry)
                    .indicesOptions(IndicesOptions.lenientExpandOpen())
            val response = remoteClient.suspending(remoteClient.admin().indices()::getIndex, true)(indexReq)
            remoteIndices = response.indices.asIterable()

        } catch (e: Exception) {
            // Ideally, Calls to the remote cluster shouldn't fail and autofollow task should be able to pick-up the newly created indices
            // matching the pattern. Should be safe to retry after configured delay.
            failureCount++
            if(failureCount > 10) {
                log.error("Fetching remote indices failed with error - ${e.stackTraceToString()}")
                failureCount = 0;
            }
        }

        var currentIndices = clusterService.state().metadata().concreteAllIndices.asIterable() // All indices - open and closed on the cluster
        if(remoteIndices.intersect(currentIndices).isNotEmpty()) {
            // Log this once when we see any update on indices on the follower cluster to prevent log flood
            if(currentIndices.toSet() != trackingIndicesOnTheCluster) {
                log.info("Cannot initiate replication for the following indices from remote ($remoteCluster) as indices with " +
                        "same name already exists on the cluster ${remoteIndices.intersect(currentIndices)}")
                trackingIndicesOnTheCluster = currentIndices.toSet()
            }
        }
        remoteIndices = remoteIndices.minus(currentIndices).minus(failedIndices)

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
            request.isAutoFollowRequest = true
            val followerRole = replicationMetadata.followerContext?.user?.roles?.get(0)
            val leaderRole = replicationMetadata.leaderContext?.user?.roles?.get(0)
            if(followerRole != null && leaderRole != null) {
                request.assumeRoles = HashMap<String, String>()
                request.assumeRoles!![ReplicateIndexRequest.FOLLOWER_FGAC_ROLE] = followerRole
                request.assumeRoles!![ReplicateIndexRequest.LEADER_FGAC_ROLE] = leaderRole
            }
            val response = client.suspendExecute(replicationMetadata, ReplicateIndexAction.INSTANCE, request)
            if (!response.isAcknowledged) {
                throw ReplicationException("Failed to auto follow remote index $remoteIndex")
            }
        } catch (e: ElasticsearchSecurityException) {
            // For permission related failures, Adding as part of failed indices as autofollow role doesn't have required permissions.
            log.trace("Cannot start replication on $remoteIndex due to missing permissions $e")
            failedIndices.add(remoteIndex)
        } catch (e: Exception) {
            // Any failure other than security exception can be safely retried and not adding to the failed indices
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
