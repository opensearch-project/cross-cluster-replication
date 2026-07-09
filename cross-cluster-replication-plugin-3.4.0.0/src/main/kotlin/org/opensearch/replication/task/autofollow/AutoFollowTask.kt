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

package org.opensearch.replication.task.autofollow

import org.opensearch.replication.ReplicationSettings
import org.opensearch.replication.action.index.ReplicateIndexAction
import org.opensearch.replication.action.index.ReplicateIndexRequest
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.metadata.store.ReplicationMetadata
import org.opensearch.replication.task.CrossClusterReplicationTask
import org.opensearch.replication.task.ReplicationState
import org.opensearch.replication.util.stackTraceToString
import org.opensearch.replication.util.suspendExecute
import org.opensearch.replication.util.suspending
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import org.opensearch.OpenSearchException
import org.opensearch.OpenSearchSecurityException
import org.opensearch.ResourceNotFoundException
import org.opensearch.action.admin.indices.get.GetIndexRequest
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.action.support.IndicesOptions
import org.opensearch.transport.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.common.logging.Loggers
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.persistent.PersistentTaskState
import org.opensearch.replication.ReplicationException
import org.opensearch.replication.action.status.ReplicationStatusAction
import org.opensearch.replication.action.status.ShardInfoRequest
import org.opensearch.replication.action.status.ShardInfoResponse
import org.opensearch.core.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.core.tasks.TaskId
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.TimeUnit

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

    override val leaderAlias = params.leaderCluster
    val patternName = params.patternName
    override val followerIndexName: String = params.patternName //Special case for auto follow
    override val log = Loggers.getLogger(javaClass, leaderAlias)
    private var trackingIndicesOnTheCluster = setOf<String>()
    private var replicationJobsQueue = ConcurrentSkipListSet<String>() // To keep track of outstanding replication jobs for this autofollow task
    private var retryScheduler: Scheduler.ScheduledCancellable? = null
    lateinit var stat: AutoFollowStat

    override suspend fun execute(scope: CoroutineScope, initialState: PersistentTaskState?) {
        stat = AutoFollowStat(params.patternName, replicationMetadata.leaderContext.resource)
        while (scope.isActive) {
            try {
                addRetryScheduler()
                pollForIndices()
                stat.lastExecutionTime = System.currentTimeMillis()
                delay(replicationSettings.autofollowFetchPollDuration.millis)
            }
            catch(e: OpenSearchException) {
                // Any transient error encountered during auto follow execution should be re-tried
                val status = e.status().status
                if(status < 500 && status != RestStatus.TOO_MANY_REQUESTS.status) {
                    log.error("Exiting autofollow task", e)
                    throw e
                }
                log.debug("Encountered transient error while running autofollow task", e)
                delay(replicationSettings.autofollowFetchPollDuration.millis)
            }
        }
    }

    private fun addRetryScheduler() {
        log.debug("Adding retry scheduler")
        if(retryScheduler != null && retryScheduler!!.getDelay(TimeUnit.NANOSECONDS) > 0L) {
            return
        }
         try {
            retryScheduler = threadPool.schedule(
                    {
                        log.debug("Clearing failed indices to schedule for the next retry")
                        stat.failedIndices.clear()
                    },
                    replicationSettings.autofollowRetryPollDuration,
                    ThreadPool.Names.SAME)
        } catch (e: Exception) {
            log.error("Error scheduling retry on failed autofollow indices ${e.stackTraceToString()}")
             retryScheduler = null
        }
    }

    override suspend fun cleanup() {
        retryScheduler?.cancel()
    }

    private suspend fun pollForIndices() {
        log.debug("Checking $leaderAlias under pattern name $patternName for new indices to auto follow")
        val entry = replicationMetadata.leaderContext.resource

        // Fetch remote indices matching auto follow pattern
        var remoteIndices = Iterable { emptyArray<String>().iterator() }
        try {
            val remoteClient = client.getRemoteClusterClient(leaderAlias)
            val indexReq = GetIndexRequest().features(*emptyArray())
                    .indices(entry)
                    .indicesOptions(IndicesOptions.lenientExpandOpen())
            val response = remoteClient.suspending(remoteClient.admin().indices()::getIndex, true)(indexReq)
            remoteIndices = response.indices.asIterable()

        } catch (e: Exception) {
            // Ideally, Calls to the remote cluster shouldn't fail and autofollow task should be able to pick-up the newly created indices
            // matching the pattern. Should be safe to retry after configured delay.
            if(stat.failedLeaderCall >= 0 && stat.failedLeaderCall.rem(10) == 0L) {
                log.error("Fetching remote indices failed with error - ${e.stackTraceToString()}")
            }
            stat.failedLeaderCall++
        }

        var currentIndices = clusterService.state().metadata().concreteAllIndices.asIterable() // All indices - open and closed on the cluster
        if(remoteIndices.intersect(currentIndices).isNotEmpty()) {
            // Log this once when we see any update on indices on the follower cluster to prevent log flood
            if(currentIndices.toSet() != trackingIndicesOnTheCluster) {
                log.info("Cannot initiate replication for the following indices from leader ($leaderAlias) as indices with " +
                        "same name already exists on the cluster ${remoteIndices.intersect(currentIndices)}")
                trackingIndicesOnTheCluster = currentIndices.toSet()
            }
        }
        remoteIndices = remoteIndices.minus(currentIndices).minus(stat.failedIndices).minus(replicationJobsQueue)

        stat.failCounterForRun = 0
        startReplicationJobs(remoteIndices)
        stat.failCount = stat.failCounterForRun
    }

    private suspend fun startReplicationJobs(remoteIndices: Iterable<String>) {
        val completedJobs = ConcurrentSkipListSet<String>()
        for(index in replicationJobsQueue) {
            val statusReq = ShardInfoRequest(index, false)
            try {
                val statusRes = client.suspendExecute(ReplicationStatusAction.INSTANCE, statusReq, injectSecurityContext = true)
                if(statusRes.status != ShardInfoResponse.BOOTSTRAPPING) {
                    completedJobs.add(index)
                }
            } catch (ex: ResourceNotFoundException) {
                // Replication metadata for this index no longer exists — it was likely stopped
                // or cleaned up externally while still tracked in the queue. Remove it to prevent
                // an endless ResourceNotFoundException flood on every poll cycle.
                log.warn("Replication metadata not found for tracked index '$index', removing from autofollow queue. " +
                        "Cause: ${ex.message}")
                completedJobs.add(index)
            } catch (ex: Exception) {
                log.error("Error while fetching the status for index $index", ex)
            }
        }

        // Remove the indices in "syncing" state from the queue
        replicationJobsQueue.removeAll(completedJobs)
        val concurrentJobsAllowed = replicationSettings.autofollowConcurrentJobsTriggerSize
        if(replicationJobsQueue.size >= concurrentJobsAllowed) {
            log.debug("Max concurrent replication jobs already in the queue for autofollow task[${params.patternName}]")
            return
        }

        var totalJobsToTrigger = concurrentJobsAllowed - replicationJobsQueue.size
        for (newRemoteIndex in remoteIndices) {
            if(totalJobsToTrigger <= 0) {
                break
            }
            startReplication(newRemoteIndex)
            replicationJobsQueue.add(newRemoteIndex)
            totalJobsToTrigger--
        }
    }

    private suspend fun startReplication(leaderIndex: String) {
        // Read the per-index checkpoint from INDEX metadata (updated by ShardReplicationTask and
        // preserved on stop). The AUTO_FOLLOW pattern metadata (replicationMetadata) always has
        // leaderSequenceNumber=-1 since it is never updated at the pattern level.
        var savedLeaderSeqNo = ReplicationMetadata.UNASSIGNED_SEQ_NO
        var savedFollowerSeqNo = ReplicationMetadata.UNASSIGNED_SEQ_NO
        try {
            val indexMeta = replicationMetadataManager.getIndexReplicationMetadata(leaderIndex)
            savedLeaderSeqNo = indexMeta.leaderSequenceNumber
            savedFollowerSeqNo = indexMeta.followerAppliedSequence
            log.debug("Found saved checkpoint for $leaderIndex: " +
                    "leader=$savedLeaderSeqNo follower=$savedFollowerSeqNo state=${indexMeta.overallState}")
        } catch (e: ResourceNotFoundException) {
            log.debug("No INDEX metadata found for $leaderIndex — treating as first-time replication")
        } catch (e: Exception) {
            log.warn("Could not read INDEX metadata for $leaderIndex: ${e.message}")
        }

        // Role transition: checkpoint exists AND the local index is present (warm-attach possible).
        // The local index exists because this cluster was the leader before the role switch.
        val isRoleTransition = replicationMetadata.checkpointPersistenceEnabled &&
                replicationMetadata.roleTransitionResumeMode == "CHECKPOINT" &&
                savedLeaderSeqNo > ReplicationMetadata.UNASSIGNED_SEQ_NO &&
                clusterService.state().metadata().hasIndex(leaderIndex)

        if (clusterService.state().metadata().hasIndex(leaderIndex)) {
            if (isRoleTransition) {
                // The local index exists because this cluster was the leader before the role switch.
                // We proceed with the warm-attach path: no snapshot restore, existing data reused.
                log.info("Role transition resume detected for $leaderAlias:$leaderIndex — " +
                        "local index exists, will attach as follower from checkpoint " +
                        "leaderSeqNo=$savedLeaderSeqNo followerSeqNo=$savedFollowerSeqNo")
            } else {
                log.info("""Cannot replicate $leaderAlias:$leaderIndex as an index with the same name already 
                        |exists.""".trimMargin())
                return
            }
        }

        var successStart = false

        try {
            log.info("Auto follow starting replication from ${leaderAlias}:$leaderIndex -> $leaderIndex")
            val request = ReplicateIndexRequest(leaderIndex, leaderAlias, leaderIndex)
            request.isAutoFollowRequest = true
            request.isRoleTransitionResume = isRoleTransition
            val followerRole = replicationMetadata.followerContext.user?.roles?.get(0)
            val leaderRole = replicationMetadata.leaderContext.user?.roles?.get(0)
            if (followerRole != null && leaderRole != null) {
                request.useRoles = HashMap()
                request.useRoles!![ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE] = followerRole
                request.useRoles!![ReplicateIndexRequest.LEADER_CLUSTER_ROLE] = leaderRole
            }
            request.settings = replicationMetadata.settings

            // For role transitions: TransportReplicateIndexAction self-detects the scenario
            // (local index is non-follower + leader has REPLICATED_INDEX_SETTING) and allows
            // the warm-attach to proceed without needing explicit leader setting cleanup.
            if (isRoleTransition) {
                log.info(
                    "Role transition: resuming replication from checkpoint — " +
                    "leaderSeqNo=$savedLeaderSeqNo, " +
                    "followerAppliedSeqNo=$savedFollowerSeqNo (no full re-sync)"
                )
            }

            val response = client.suspendExecute(replicationMetadata, ReplicateIndexAction.INSTANCE, request)
            if (!response.isAcknowledged) {
                throw ReplicationException("Failed to auto follow leader index $leaderIndex")
            }
            successStart = true
            log.debug("Auto follow has started replication from ${leaderAlias}:$leaderIndex -> $leaderIndex")
        } catch (e: OpenSearchSecurityException) {
            log.trace("Cannot start replication on $leaderIndex due to missing permissions $e")
        } catch (e: Exception) {
            log.warn("Failed to start replication for $leaderAlias:$leaderIndex -> $leaderIndex.", e)
        } finally {
            if (successStart) {
                stat.successCount++
                stat.failedIndices.remove(leaderIndex)
            } else {
                stat.failCounterForRun++
                stat.failedIndices.add(leaderIndex)
            }
        }
    }

    /**
     * Clears the REPLICATED_INDEX_SETTING on the leader index via the remote client.
     * After a role switch the new leader's index (old follower) still carries this setting,
     * which blocks TransportReplicateIndexAction's "cannot replicate a replicated index" guard.
     */
    private suspend fun clearLeaderReplicatedSetting(leaderIndex: String) {
        try {
            val remoteClient = client.getRemoteClusterClient(leaderAlias)
            val getSettingsReq = GetSettingsRequest().indices(leaderIndex)
            val settingsResp = remoteClient.suspending(
                remoteClient.admin().indices()::getSettings, defaultContext = true)(getSettingsReq)
            val settingValue = settingsResp.getSetting(leaderIndex,
                org.opensearch.replication.ReplicationPlugin.REPLICATED_INDEX_SETTING.key)
            if (!settingValue.isNullOrBlank()) {
                log.info("Clearing REPLICATED_INDEX_SETTING on $leaderAlias:$leaderIndex for role transition")
                val updateReq = UpdateSettingsRequest(leaderIndex)
                    .settings(org.opensearch.common.settings.Settings.builder()
                        .putNull(org.opensearch.replication.ReplicationPlugin.REPLICATED_INDEX_SETTING.key))
                remoteClient.suspending(
                    remoteClient.admin().indices()::updateSettings, defaultContext = true)(updateReq)
                log.info("Successfully cleared REPLICATED_INDEX_SETTING on $leaderAlias:$leaderIndex")
            }
        } catch (e: Exception) {
            log.warn("Could not clear REPLICATED_INDEX_SETTING on $leaderAlias:$leaderIndex: ${e.message}")
            throw e
        }
    }


    override fun toString(): String {
        return "AutoFollowTask(from=${leaderAlias} with pattern=${params.patternName})"
    }

    override fun replicationTaskResponse(): CrossClusterReplicationTaskResponse {
        return CrossClusterReplicationTaskResponse(ReplicationState.COMPLETED.name)
    }

    override fun getStatus(): AutoFollowStat {
        return stat
    }

    override suspend fun setReplicationMetadata() {
        this.replicationMetadata = replicationMetadataManager.getAutofollowMetadata(followerIndexName, leaderAlias, fetch_from_primary = true)
    }
}

class AutoFollowStat: Task.Status {
    companion object {
        val NAME = "autofollow_stat"
    }

    val name :String
    val pattern :String
    var failCount: Long=0
    var failedIndices = ConcurrentSkipListSet<String>() // Failed indices for replication from this autofollow task
    var failCounterForRun :Long=0
    var successCount: Long=0
    var failedLeaderCall :Long=0
    var lastExecutionTime : Long=0


    constructor(name: String, pattern: String) {
        this.name = name
        this.pattern = pattern
    }

    constructor(inp: StreamInput) {
        name = inp.readString()
        pattern = inp.readString()
        failCount = inp.readLong()
        val inpFailedIndices = inp.readList(StreamInput::readString)
        failedIndices = ConcurrentSkipListSet<String>(inpFailedIndices)
        successCount = inp.readLong()
        failedLeaderCall = inp.readLong()
        lastExecutionTime = inp.readLong()
    }

    override fun writeTo(out: StreamOutput) {
       out.writeString(name)
       out.writeString(pattern)
       out.writeLong(failCount)
       out.writeCollection(failedIndices, StreamOutput::writeString)
       out.writeLong(successCount)
       out.writeLong(failedLeaderCall)
       out.writeLong(lastExecutionTime)
    }

    override fun getWriteableName(): String {
        return NAME
    }

    override fun toXContent(builder: XContentBuilder, p1: ToXContent.Params?): XContentBuilder {
        builder.startObject()
        builder.field("name", name)
        builder.field("pattern", pattern)
        builder.field("num_success_start_replication", successCount)
        builder.field("num_failed_start_replication", failCount)
        builder.field("num_failed_leader_calls", failedLeaderCall)
        builder.field("failed_indices", failedIndices)
        builder.field("last_execution_time", lastExecutionTime)
        return builder.endObject()
    }

}
