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
import org.opensearch.replication.util.StaleTaskUtils
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.removeTask
import org.opensearch.replication.util.startTask
import org.opensearch.replication.util.suspending
import org.opensearch.replication.util.waitForClusterStateUpdate
import org.opensearch.replication.util.waitForTaskCondition
import org.opensearch.replication.ReplicationPlugin.Companion.REPLICATED_INDEX_SETTING
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
import org.opensearch.cluster.AckedClusterStateUpdateTask
import org.opensearch.cluster.ClusterState
import org.opensearch.common.Priority
import org.opensearch.common.unit.TimeValue
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.common.settings.Settings
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.common.settings.IndexScopedSettings
import org.opensearch.index.IndexNotFoundException
import org.opensearch.persistent.PersistentTasksCustomMetadata
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

                var isWarmAttach = false
                if (state.routingTable.hasIndex(replicateIndexReq.followerIndex)) {
                    // If the index is already a CCR follower (REPLICATED_INDEX_SETTING is set),
                    // reject — the user must stop replication first.
                    // If the index exists but is NOT a follower (e.g. it was previously the leader
                    // in a role-transition scenario), allow it to proceed as a warm-attach:
                    // IndexReplicationTask.isResumed() will detect the existing index and skip the
                    // snapshot restore, letting shard tasks resume from the local checkpoint.
                    //
                    // SECURITY: the local index missing REPLICATED_INDEX_SETTING is, by itself, a weak
                    // signal — any pre-existing unrelated index with the same name would satisfy it.
                    // We additionally require the caller to have explicitly asserted
                    // isRoleTransitionResume=true on the request, which AutoFollowTask only sets after
                    // independently verifying a real persisted checkpoint for this index. Without this
                    // second check, an existing regular index could be silently repurposed as a CCR
                    // follower (and potentially re-enable circular replication with the leader).
                    val existingFollowerSetting = state.metadata()
                        .index(replicateIndexReq.followerIndex)
                        ?.settings?.get(ReplicationPlugin.REPLICATED_INDEX_SETTING.key)
                    if (!existingFollowerSetting.isNullOrBlank()) {
                        throw IllegalArgumentException("Cant use same index again for replication. " +
                                "Delete the index:${replicateIndexReq.followerIndex}")
                    }
                    if (!replicateIndexReq.isRoleTransitionResume || !replicateIndexReq.isAutoFollowRequest) {
                        // Defense in depth: the only legitimate producer of isRoleTransitionResume=true is
                        // AutoFollowTask, which always sets isAutoFollowRequest=true in the same request and
                        // has independently verified a persisted checkpoint before setting this flag. Requiring
                        // both flags here ensures this bypass can never be reached via a directly-constructed
                        // ReplicateIndexRequest that skips TransportReplicateIndexAction's SetupChecksAction
                        // (which is itself only skipped when isAutoFollowRequest=true).
                        throw IllegalArgumentException("Cant use same index again for replication. " +
                                "Delete the index:${replicateIndexReq.followerIndex}")
                    }
                    isWarmAttach = true
                    log.info("Index ${replicateIndexReq.followerIndex} exists locally but is not a " +
                            "current follower and request explicitly asserts isRoleTransitionResume — " +
                            "proceeding as warm-attach role-transition resume")
                }

                // Remove all replication tasks before creating new ones
                val removedTaskCount = StaleTaskUtils.removeAllTasksForIndex(clusterService, nodeClient, replicateIndexReq.followerIndex)
                if (removedTaskCount > 0) {
                    log.info("Cleaned up $removedTaskCount tasks for ${replicateIndexReq.followerIndex}")
                }

                indexScopedSettings.validate(replicateIndexReq.settings,
                        false,
                        false)

                val params = IndexReplicationParams(replicateIndexReq.leaderAlias, remoteMetadata.index, replicateIndexReq.followerIndex)

                replicationMetadataManager.addIndexReplicationMetadata(replicateIndexReq.followerIndex,
                        replicateIndexReq.leaderAlias, replicateIndexReq.leaderIndex,
                        ReplicationOverallState.RUNNING, user, replicateIndexReq.useRoles?.getOrDefault(ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE, null),
                        replicateIndexReq.useRoles?.getOrDefault(ReplicateIndexRequest.LEADER_CLUSTER_ROLE, null), replicateIndexReq.settings)

                if (isWarmAttach) {
                    // Defense in depth: re-assert the full eligibility invariant immediately before
                    // performing the privileged raw cluster-state mutation below. REPLICATED_INDEX_SETTING
                    // is InternalIndex-scoped and therefore cannot be set via the normal, validated and
                    // audited TransportUpdateSettingsAction path — that is precisely why this code stamps
                    // it directly via AckedClusterStateUpdateTask. Because that bypasses the usual
                    // settings-update validation/audit trail, we fail closed here if any of the invariants
                    // that gated isWarmAttach above no longer hold (e.g. due to a future refactor), rather
                    // than silently proceeding with a privileged mutation.
                    check(replicateIndexReq.isRoleTransitionResume && replicateIndexReq.isAutoFollowRequest) {
                        "Refusing to stamp $REPLICATED_INDEX_SETTING on ${replicateIndexReq.followerIndex}: " +
                                "warm-attach invariant (isRoleTransitionResume && isAutoFollowRequest) not satisfied"
                    }

                    // Explicit audit trail entry: this mutation does not flow through
                    // TransportUpdateSettingsAction, so the security plugin's action-based audit log will
                    // not record it. Log the identity of the authenticated user, along with the exact
                    // index and leader resource involved, so this privileged operation remains traceable.
                    log.info("AUDIT: user=${user?.name ?: "system"} performing privileged internal stamp of " +
                            "$REPLICATED_INDEX_SETTING=${replicateIndexReq.leaderAlias}:${replicateIndexReq.leaderIndex} " +
                            "on local index=${replicateIndexReq.followerIndex} (warm-attach role-transition resume)")
                    val stampResponse = clusterService.waitForClusterStateUpdate<AcknowledgedResponse>(
                            "warm-attach-replicated-setting-${replicateIndexReq.followerIndex}") { l ->
                        object : AckedClusterStateUpdateTask<AcknowledgedResponse>(Priority.NORMAL, null, l) {
                            override fun timeout(): TimeValue = TimeValue.timeValueSeconds(60)
                            override fun ackTimeout(): TimeValue = TimeValue.timeValueSeconds(60)
                            override fun execute(currentState: ClusterState): ClusterState {
                                val idxMeta = currentState.metadata().index(replicateIndexReq.followerIndex)
                                    ?: return currentState
                                // Race-safety: re-verify at mutation time (not just at the earlier check
                                // against a possibly-stale `state` snapshot) that this index still does not
                                // carry a REPLICATED_INDEX_SETTING. If it does — e.g. a concurrent request
                                // legitimately started replication on it in the meantime — refuse to
                                // overwrite/clobber that value with this warm-attach's leader reference.
                                val currentSetting = idxMeta.settings.get(REPLICATED_INDEX_SETTING.key)
                                if (!currentSetting.isNullOrBlank()) {
                                    log.warn("Aborting warm-attach stamp for ${replicateIndexReq.followerIndex}: " +
                                            "$REPLICATED_INDEX_SETTING is already set to '$currentSetting' " +
                                            "(concurrent modification) — leaving cluster state unchanged")
                                    return currentState
                                }
                                val newSettings = Settings.builder()
                                    .put(idxMeta.settings)
                                    .put(REPLICATED_INDEX_SETTING.key,
                                        "${replicateIndexReq.leaderAlias}:${replicateIndexReq.leaderIndex}")
                                    .build()
                                val newIdxMeta = IndexMetadata.builder(idxMeta)
                                    .settings(newSettings)
                                    .settingsVersion(idxMeta.settingsVersion + 1)
                                val mdBuilder = Metadata.builder(currentState.metadata()).put(newIdxMeta)
                                return ClusterState.builder(currentState).metadata(mdBuilder).build()
                            }
                            override fun newResponse(acknowledged: Boolean) = AcknowledgedResponse(acknowledged)
                        }
                    }
                    if (!stampResponse.isAcknowledged) {
                        log.warn("REPLICATED_INDEX_SETTING stamp was not acknowledged for ${replicateIndexReq.followerIndex} — continuing anyway")
                    } else {
                        log.info("REPLICATED_INDEX_SETTING successfully stamped on ${replicateIndexReq.followerIndex}")
                    }
                }

                log.debug("Starting index replication task in persistent task service with name: replication:index:${replicateIndexReq.followerIndex}")

                // For warm-attach (role-transition), a previous failed attempt may have left a stale
                // persistent task in the cluster state. Remove it before starting a fresh one to avoid
                // ResourceAlreadyExistsException.
                val staleTaskId = "replication:index:${replicateIndexReq.followerIndex}"
                val existingTask = PersistentTasksCustomMetadata.getTaskWithId<IndexReplicationParams>(state, staleTaskId)
                if (existingTask != null) {
                    log.info("Found existing replication task $staleTaskId — removing before warm-attach restart")
                    try {
                        persistentTasksService.removeTask(staleTaskId)
                        log.info("Removed stale task $staleTaskId")
                    } catch (e: Exception) {
                        log.warn("Could not remove stale task $staleTaskId: ${e.message}")
                    }
                }

                val task = persistentTasksService.startTask("replication:index:${replicateIndexReq.followerIndex}",
                        IndexReplicationExecutor.TASK_NAME, params)

                if (!task.isAssigned) {
                    log.error("Failed to assign task")
                    listener.onResponse(ReplicateIndexResponse(false))
                    return@launch
                }

                // For warm-attach (role-transition), the index already exists locally and the task
                // will stamp REPLICATED_INDEX_SETTING and start shard tasks asynchronously.
                // Stamping requires a cluster state update that can take longer than the default 30s
                // request timeout, causing a spurious timeout even though replication is progressing.
                // Since the task is assigned and running, return success immediately and let it
                // proceed asynchronously — use _status to track progress.
                if (isWarmAttach) {
                    log.info("Warm-attach role-transition: task ${task.id} assigned — returning success asynchronously")
                    listener.onResponse(AcknowledgedResponse(true))
                    return@launch
                }

                log.info("Persistent task created for replication: follower=${replicateIndexReq.followerIndex}, leader=${replicateIndexReq.leaderAlias}:${replicateIndexReq.leaderIndex}, taskId=${task.id}")
                log.debug("Waiting for persistent task to move to following state")
                // Normal start: wait until replication is effectively started.
                persistentTasksService.waitForTaskCondition(task.id, replicateIndexReq.timeout()) { t ->
                    val replicationState = (t?.state as? IndexReplicationState)?.state
                    when (replicationState) {
                        ReplicationState.FOLLOWING -> true
                        ReplicationState.RESTORING -> !replicateIndexReq.waitForRestore
                        ReplicationState.INIT_FOLLOW,
                        ReplicationState.MONITORING -> true
                        else -> false
                    }
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

    /**
     * Fetches remote index metadata for multiple indices in ONE remote call for bulk API
     * Returns a map of index name -> IndexMetadata.
     */
    internal suspend fun getRemoteIndexMetadata(leaderAlias: String, indices: List<String>): Map<String, IndexMetadata> {
        val remoteClusterClient = nodeClient.getRemoteClusterClient(leaderAlias)
        val clusterStateRequest = remoteClusterClient.admin().cluster().prepareState()
                .clear()
                .setIndices(*indices.toTypedArray())
                .setMetadata(true)
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .request()
        val remoteState = remoteClusterClient.suspending(remoteClusterClient.admin().cluster()::state,
                injectSecurityContext = true, defaultContext = true)(clusterStateRequest).state
        return indices.mapNotNull { index ->
            remoteState.metadata().index(index)?.let { index to it }
        }.toMap()
    }
}
