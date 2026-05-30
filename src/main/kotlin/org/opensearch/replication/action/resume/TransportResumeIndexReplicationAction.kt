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

package org.opensearch.replication.action.resume

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.ResourceAlreadyExistsException
import org.opensearch.ResourceNotFoundException
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.commons.replication.action.ReplicationActions.STOP_REPLICATION_ACTION_TYPE
import org.opensearch.commons.replication.action.StopIndexReplicationRequest
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.index.shard.ShardId
import org.opensearch.env.Environment
import org.opensearch.index.IndexNotFoundException
import org.opensearch.replication.action.index.ReplicateIndexAction
import org.opensearch.replication.action.index.ReplicateIndexRequest
import org.opensearch.replication.action.index.ReplicateIndexResponse
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.metadata.ReplicationOverallState
import org.opensearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import org.opensearch.replication.metadata.state.getReplicationStateParamsForIndex
import org.opensearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import org.opensearch.replication.task.ReplicationState
import org.opensearch.replication.task.index.IndexReplicationExecutor
import org.opensearch.replication.task.index.IndexReplicationParams
import org.opensearch.replication.task.index.IndexReplicationState
import org.opensearch.replication.util.*
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import java.io.IOException


class TransportResumeIndexReplicationAction @Inject constructor(transportService: TransportService,
                                                                clusterService: ClusterService,
                                                                threadPool: ThreadPool,
                                                                actionFilters: ActionFilters,
                                                                indexNameExpressionResolver: IndexNameExpressionResolver,
                                                                val client: Client,
                                                                val replicationMetadataManager: ReplicationMetadataManager,
                                                                private val environment: Environment) :
    TransportClusterManagerNodeAction<ResumeIndexReplicationRequest, AcknowledgedResponse> (ResumeIndexReplicationAction.NAME,
            transportService, clusterService, threadPool, actionFilters, ::ResumeIndexReplicationRequest,
            indexNameExpressionResolver), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportResumeIndexReplicationAction::class.java)
    }

    override fun checkBlock(request: ResumeIndexReplicationRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    @Throws(Exception::class)
    override fun clusterManagerOperation(request: ResumeIndexReplicationRequest, state: ClusterState,
                                 listener: ActionListener<AcknowledgedResponse>) {
        launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            listener.completeWith {
                log.info("Resuming index replication on index:" + request.indexName)
                validateResumeReplicationRequest(request)

                // Remove all existing tasks before resuming to ensure a clean slate
                StaleTaskUtils.removeAllTasksForIndex(clusterService, client, request.indexName)

                val replMetadata = replicationMetadataManager.getIndexReplicationMetadata(request.indexName)
                val remoteMetadata = getLeaderIndexMetadata(replMetadata.connectionName, replMetadata.leaderContext.resource)
                val params = IndexReplicationParams(replMetadata.connectionName, remoteMetadata.index, request.indexName)
                val resumable = isResumable(params)

                if (!resumable && !request.forceResume) {
                    throw ResourceNotFoundException(
                        "Retention lease doesn't exist. Replication can't be resumed for ${request.indexName}. " +
                                "Use force_resume=true to restore from snapshot."
                    )
                }

                if (!resumable && request.forceResume) {
                    // Force resume via internal stop + start.
                    // This reuses the battle-tested stop and start paths, ensuring proper
                    // cleanup of metadata, checkpoints, tasks, and index blocks.
                    executeForceResume(request.indexName, replMetadata, remoteMetadata)
                    return@completeWith AcknowledgedResponse(true)
                }

                // Normal resume path (retention leases are valid)
                // forceResume=true with valid leases also takes this path (no-op, just resume normally)

                val remoteClient = client.getRemoteClusterClient(params.leaderAlias)
                val getSettingsRequest = GetSettingsRequest().includeDefaults(false).indices(params.leaderIndex.name)
                val settingsResponse = remoteClient.suspending(
                    remoteClient.admin().indices()::getSettings,
                    injectSecurityContext = true
                )(getSettingsRequest)

                val leaderSettings = settingsResponse.indexToSettings.get(params.leaderIndex.name) ?: throw IndexNotFoundException(params.leaderIndex.name)

                // Disabling knn checks as new api call will require us add roles in security index which will be a breaking call.
//                ValidationUtil.checkKNNEligibility(nodesInfoResponse, params.leaderIndex.name)

                ValidationUtil.validateAnalyzerSettings(environment, leaderSettings, replMetadata.settings)

                replicationMetadataManager.updateIndexReplicationState(request.indexName, ReplicationOverallState.RUNNING)
                val task = persistentTasksService.startTask("replication:index:${request.indexName}",
                        IndexReplicationExecutor.TASK_NAME, params)

                if (!task.isAssigned) {
                    log.error("Failed to assign task")
                    listener.onResponse(ReplicateIndexResponse(false))
                }

                // Now wait for the replication to start and the follower index to get created before returning
                persistentTasksService.waitForTaskCondition(task.id, request.timeout()) { t ->
                    val replicationState = (t.state as IndexReplicationState?)?.state
                    replicationState == ReplicationState.FOLLOWING
                }

                AcknowledgedResponse(true)
            }
        }
    }

    /**
     * Executes force resume by internally performing stop + delete + start.
     * Reuses the well-tested stop and start paths for proper cleanup and fresh bootstrap.
     * The RemoteClusterRepository restore path handles retention lease acquisition
     * during the snapshot restore, so no pre-restore lease step is needed.
     */
    private suspend fun executeForceResume(
        followerIndexName: String,
        replMetadata: org.opensearch.replication.metadata.store.ReplicationMetadata,
        leaderIndexMetadata: IndexMetadata
    ) {
        val connectionName = replMetadata.connectionName
        val leaderIndexName = replMetadata.leaderContext.resource
        val settings = replMetadata.settings
        val followerUser = replMetadata.followerContext.user
        val leaderUser = replMetadata.leaderContext.user

        log.info("Force resume for $followerIndexName via internal stop + start. Leader=$connectionName:$leaderIndexName")

        // Step 1: Full stop — cleans up tasks, leases, metadata, blocks, cluster state
        client.suspendExecute(STOP_REPLICATION_ACTION_TYPE, StopIndexReplicationRequest(followerIndexName), defaultContext = true)
        log.info("Force resume: replication stopped for $followerIndexName")

        // Step 2: Delete the follower index so start replication can create it fresh.
        // After stop, the follower is a standalone index. Start replication requires
        // the follower index to NOT exist (it creates it via snapshot restore).
        if (clusterService.state().routingTable.hasIndex(followerIndexName)) {
            client.suspending(client.admin().indices()::delete, defaultContext = true)(
                org.opensearch.action.admin.indices.delete.DeleteIndexRequest(followerIndexName)
            )
            log.info("Force resume: deleted standalone follower index $followerIndexName")
        }

        // Step 3: Full start — fresh metadata, task creation, snapshot restore.
        // The RemoteClusterRepository restore path acquires retention leases during
        // the snapshot restore process, so no separate lease acquisition is needed.
        val startRequest = ReplicateIndexRequest(followerIndexName, connectionName, leaderIndexName, settings)
        // Skip setup checks since this is an internal re-start (user already had permissions
        // when replication was originally set up). Same pattern as autofollow.
        startRequest.isAutoFollowRequest = true
        if (followerUser != null && leaderUser != null) {
            startRequest.useRoles = hashMapOf(
                ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE to followerUser.name,
                ReplicateIndexRequest.LEADER_CLUSTER_ROLE to leaderUser.name
            )
        }
        client.suspendExecute(ReplicateIndexAction.INSTANCE, startRequest, defaultContext = true)
        log.info("Force resume: replication restarted for $followerIndexName")
    }

    private suspend fun isResumable(params: IndexReplicationParams): Boolean {
        var isResumable = true
        val remoteClient = client.getRemoteClusterClient(params.leaderAlias)
        val shards = clusterService.state().routingTable.indicesRouting().get(params.followerIndexName)?.shards()
        val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(clusterService.clusterName.value(), clusterService.state().metadata.clusterUUID(), remoteClient)
        shards?.forEach {
            val followerShardId = it.value.shardId
            if (!retentionLeaseHelper.verifyRetentionLeaseExist(ShardId(params.leaderIndex, followerShardId.id), followerShardId)) {
                log.info("Retention lease missing for follower=${params.followerIndexName}, followerShard=$followerShardId, leaderShard=${ShardId(params.leaderIndex, followerShardId.id)} - replication cannot be resumed")
                isResumable = false
            }
        }

        if (isResumable) {
            return true
        }

        // clean up all retention leases we may have accidentally took while doing verifyRetentionLeaseExist .
        // Idempotent Op which does no harm
        shards?.forEach {
            val followerShardId = it.value.shardId
            log.debug("Removing lease for $followerShardId.id ")
            retentionLeaseHelper.attemptRetentionLeaseRemoval(ShardId(params.leaderIndex, followerShardId.id), followerShardId)
        }

        return false
    }

    private suspend fun getLeaderIndexMetadata(leaderAlias: String, leaderIndex: String): IndexMetadata {
        val leaderClusterClient = client.getRemoteClusterClient(leaderAlias)
        val clusterStateRequest = leaderClusterClient.admin().cluster().prepareState()
                .clear()
                .setIndices(leaderIndex)
                .setMetadata(true)
                .setIndicesOptions(IndicesOptions.strictSingleIndexNoExpandForbidClosed())
                .request()
        val remoteState = leaderClusterClient.suspending(leaderClusterClient.admin().cluster()::state)(clusterStateRequest).state
        return remoteState.metadata.index(leaderIndex) ?: throw IndexNotFoundException("${leaderAlias}:${leaderIndex}")
    }

    private fun validateResumeReplicationRequest(request: ResumeIndexReplicationRequest) {
        val replicationStateParams = getReplicationStateParamsForIndex(clusterService, request.indexName)
                ?:
            throw IllegalArgumentException("No replication in progress for index:${request.indexName}")
        val replicationOverallState = replicationStateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE]

        if (replicationOverallState != ReplicationOverallState.PAUSED.name)
            throw ResourceAlreadyExistsException("Replication on Index ${request.indexName} is already running")
    }

    override fun executor(): String {
        return ThreadPool.Names.SAME
    }

    @Throws(IOException::class)
    override fun read(inp: StreamInput): AcknowledgedResponse {
        return AcknowledgedResponse(inp)
    }
}
