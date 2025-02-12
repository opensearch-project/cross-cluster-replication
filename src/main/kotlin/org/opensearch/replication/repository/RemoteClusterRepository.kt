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

package org.opensearch.replication.repository

import org.opensearch.replication.ReplicationPlugin
import org.opensearch.replication.ReplicationSettings
import org.opensearch.replication.action.repository.GetStoreMetadataAction
import org.opensearch.replication.action.repository.GetStoreMetadataRequest
import org.opensearch.replication.action.repository.ReleaseLeaderResourcesAction
import org.opensearch.replication.action.repository.ReleaseLeaderResourcesRequest
import org.opensearch.replication.util.restoreShardWithRetries
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.metadata.store.ReplicationMetadata
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.execute
import org.opensearch.replication.util.suspendExecute
import kotlinx.coroutines.Dispatchers
import org.apache.logging.log4j.LogManager
import org.apache.lucene.index.IndexCommit
import org.opensearch.Version
import org.opensearch.core.action.ActionListener
import org.opensearch.action.ActionRequest
import org.opensearch.core.action.ActionResponse
import org.opensearch.action.ActionType
import org.opensearch.action.admin.indices.stats.IndicesStatsAction
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.support.IndicesOptions
import org.opensearch.transport.client.Client
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.ClusterStateUpdateTask
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.metadata.RepositoryMetadata
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.Nullable
import org.opensearch.common.UUIDs
import org.opensearch.common.lifecycle.AbstractLifecycleComponent
import org.opensearch.common.metrics.CounterMetric
import org.opensearch.common.settings.Settings
import org.opensearch.index.mapper.MapperService
import org.opensearch.core.index.shard.ShardId
import org.opensearch.index.snapshots.IndexShardSnapshotStatus
import org.opensearch.index.store.Store
import org.opensearch.indices.recovery.RecoverySettings
import org.opensearch.indices.recovery.RecoveryState
import org.opensearch.replication.ReplicationPlugin.Companion.REPLICATION_INDEX_TRANSLOG_PRUNING_ENABLED_SETTING
import org.opensearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import org.opensearch.replication.util.stackTraceToString
import org.opensearch.repositories.IndexId
import org.opensearch.repositories.Repository
import org.opensearch.repositories.RepositoryData
import org.opensearch.repositories.RepositoryShardId
import org.opensearch.repositories.ShardGenerations
import org.opensearch.snapshots.SnapshotId
import org.opensearch.snapshots.SnapshotInfo
import org.opensearch.snapshots.SnapshotState
import org.opensearch.transport.ConnectTransportException
import org.opensearch.transport.NodeDisconnectedException
import org.opensearch.transport.NodeNotConnectedException
import java.util.UUID
import java.util.function.Consumer
import java.util.function.Function
import kotlin.collections.ArrayList
import org.opensearch.common.Priority

const val REMOTE_REPOSITORY_PREFIX = "replication-remote-repo-"
const val REMOTE_REPOSITORY_TYPE = "replication-remote-repository"
const val REMOTE_SNAPSHOT_NAME = "replication-remote-snapshot"

class RemoteClusterRepository(private val repositoryMetadata: RepositoryMetadata,
                              private val client: Client,
                              private val clusterService: ClusterService,
                              private val recoverySettings: RecoverySettings,
                              private val replicationMetadataManager: ReplicationMetadataManager,
                              private val replicationSettings: ReplicationSettings) : AbstractLifecycleComponent(), Repository, CoroutineScope by GlobalScope {

    // Lazy init because we initialize when a leader cluster seed setting is added at which point the leader
    // cluster connection might not be available yet
    private val leaderClusterClient by lazy { client.getRemoteClusterClient(repositoryMetadata.leaderClusterName()) }

    companion object {
        private val log = LogManager.getLogger(RemoteClusterRepository::class.java)
        private val restoreRateLimitingTimeInNanos = CounterMetric()
        private fun String.asUUID(): String = UUID.nameUUIDFromBytes(toByteArray()).toString()
        private fun RepositoryMetadata.leaderClusterName(): String = this.name().split(REMOTE_REPOSITORY_PREFIX)[1]
        const val REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC = 60000L

        fun clusterForRepo(remoteRepoName: String) = remoteRepoName.split(REMOTE_REPOSITORY_PREFIX)[1]
        fun repoForCluster(leaderClusterName: String): String = REMOTE_REPOSITORY_PREFIX + leaderClusterName
    }



    override fun getRestoreThrottleTimeInNanos(): Long {
        return restoreRateLimitingTimeInNanos.count()
    }

    override fun getRemoteUploadThrottleTimeInNanos(): Long {
        throw UnsupportedOperationException("Operation not permitted")
    }

    override fun getRemoteDownloadThrottleTimeInNanos(): Long {
        throw UnsupportedOperationException("Operation not permitted")
    }

    override fun finalizeSnapshot(shardGenerations: ShardGenerations?, repositoryStateId: Long, clusterMetadata: Metadata?,
                                  snapshotInfo: SnapshotInfo?, repositoryMetaVersion: Version?,
                                  stateTransformer: Function<ClusterState, ClusterState>?,
                                  listener: ActionListener<RepositoryData>?) {
        throw UnsupportedOperationException("Operation not permitted")
    }

    override fun finalizeSnapshot(shardGenerations: ShardGenerations?, repositoryStateId: Long, clusterMetadata: Metadata?,
                                  snapshotInfo: SnapshotInfo?, repositoryMetaVersion: Version?,
                                  stateTransformer: Function<ClusterState, ClusterState>?, repositoryUpdatePriority: Priority,
                                  listener: ActionListener<RepositoryData>?) {
        throw UnsupportedOperationException("Operation not permitted")
    }
    
    override fun deleteSnapshots(snapshotIds: MutableCollection<SnapshotId>?, repositoryStateId: Long,
                                 repositoryMetaVersion: Version?, listener: ActionListener<RepositoryData>?) {
        throw UnsupportedOperationException("Operation not permitted")
    }

    override fun startVerification(): String {
        throw UnsupportedOperationException("Operation not permitted")
    }

    override fun snapshotShard(store: Store?, mapperService: MapperService?, snapshotId: SnapshotId?, indexId: IndexId?,
                               snapshotIndexCommit: IndexCommit?, @Nullable shardStateIdentifier: String?,
                               snapshotStatus: IndexShardSnapshotStatus?, repositoryMetaVersion: Version?,
                               userMetadata: MutableMap<String, Any>?, listener: ActionListener<String>?) {
        throw UnsupportedOperationException("Operation not permitted")
    }

    override fun getMetadata(): RepositoryMetadata {
        return repositoryMetadata
    }

    override fun verify(verificationToken: String, localNode: DiscoveryNode) {
    }

    override fun cloneShardSnapshot(source: SnapshotId?, target: SnapshotId?, shardId: RepositoryShardId?, shardGeneration: String?, listener: ActionListener<String>?) {
        throw UnsupportedOperationException("Operation not permitted")
    }

    override fun doStart() {
    }

    override fun doStop() {
    }

    override fun doClose() {
    }

    override fun endVerification(verificationToken: String) {
    }

    override fun getSnapshotThrottleTimeInNanos(): Long {
        throw UnsupportedOperationException("Operation not permitted")
    }

    override fun getShardSnapshotStatus(snapshotId: SnapshotId, indexId: IndexId,
                                        shardId: ShardId): IndexShardSnapshotStatus? {
        val indicesStatsRequest = IndicesStatsRequest().all().indices(indexId.name)
        val indicesStatsResponse = leaderClusterGetAction(IndicesStatsAction.INSTANCE, indicesStatsRequest, shardId.indexName)
        for (i in indicesStatsResponse.shards.indices) {
            if (indicesStatsResponse.shards[i].shardRouting.shardId().id == shardId.id) {
                val sizeInBytes = indicesStatsResponse.shards[i].stats?.store?.sizeInBytes!!
                // Filling in dummy values except size
                return IndexShardSnapshotStatus.newDone(0L, 3L, 1,
                        1, sizeInBytes, sizeInBytes, "")
            }
        }
        return null
    }

    override fun updateState(state: ClusterState) {
        // TODO: Update any state as required
    }

    override fun executeConsistentStateUpdate(createUpdateTask: Function<RepositoryData, ClusterStateUpdateTask>?,
                                              source: String?, onFailure: Consumer<Exception>?) {
        throw UnsupportedOperationException("Operation not permitted")
    }

    /*
     * Step 1: Gets all the indices from the leader cluster.
     * At this point, we don't have information on targeted index for restore.
     * Fetches all the information and creates a repository data object for the restore workflow.
     */
    override fun getRepositoryData(listener: ActionListener<RepositoryData>) {
        val clusterState = getLeaderClusterState(false, false)
        val shardGenerations = ShardGenerations.builder()
        clusterState.metadata.indices.values
            .forEach { indexMetadata ->
                val indexId = IndexId(indexMetadata.index.name, indexMetadata.indexUUID)
                for (i in 0 until indexMetadata.numberOfShards) {
                    // Generations only make sense for eventually consistent BlobStores so just use a dummy value here.
                    shardGenerations.put(indexId, i, "dummy")
                }
            }
        val snapshotId = SnapshotId(REMOTE_SNAPSHOT_NAME, REMOTE_SNAPSHOT_NAME.asUUID())
        val repositoryData = RepositoryData.EMPTY
                .addSnapshot(snapshotId, SnapshotState.SUCCESS, Version.CURRENT, shardGenerations.build(), null, null)
        listener.onResponse(repositoryData)
    }

    /*
     * Step 2: Creates the Snapshot object to give information
     * on the indices present against the snapshotId
     */
    override fun getSnapshotInfo(snapshotId: SnapshotId): SnapshotInfo {
        val leaderClusterState = getLeaderClusterState(false, false)
        assert(REMOTE_SNAPSHOT_NAME.equals(snapshotId.name), { "SnapshotName differs" })
        val indices = leaderClusterState.metadata().indices().keys.toList()
        return SnapshotInfo(snapshotId, indices, emptyList(), SnapshotState.SUCCESS, Version.CURRENT)
    }

    /*
     * Step 3: Global metadata params are not passed in the restore workflow for this use-case
     * TODO: Implement this after analysing all the use-cases
     */
    override fun getSnapshotGlobalMetadata(snapshotId: SnapshotId): Metadata {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    /*
     * Step 4: Constructs the index metadata object for the index requested
     */
    override fun getSnapshotIndexMetaData(repositoryData: RepositoryData, snapshotId: SnapshotId, index: IndexId): IndexMetadata {
        assert(REMOTE_SNAPSHOT_NAME.equals(snapshotId.name), { "SnapshotName differs" })
        val leaderClusterState = getLeaderClusterState(false, false, index.name)
        val indexMetadata = leaderClusterState.metadata.index(index.name)

        // Add replication specific settings
        val builder = Settings.builder().put(indexMetadata.settings)
        val replicatedIndex = "${repositoryMetadata.leaderClusterName()}:${index.name}"
        builder.put(ReplicationPlugin.REPLICATED_INDEX_SETTING.key, replicatedIndex)

        // Remove translog pruning for the follower index
        builder.remove(REPLICATION_INDEX_TRANSLOG_PRUNING_ENABLED_SETTING.key)
        builder.remove(IndexMetadata.SETTING_REMOTE_STORE_ENABLED)
        builder.remove(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY)
        builder.remove(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY)

        val indexMdBuilder = IndexMetadata.builder(indexMetadata).settings(builder)
        indexMetadata.aliases.values.forEach {
            indexMdBuilder.putAlias(it)
        }
        return indexMdBuilder.build()
    }

    /*
     * Step 5: restore shard by fetching the lucene segments from the leader cluster
     */
    override fun restoreShard(store: Store, snapshotId: SnapshotId, indexId: IndexId, snapshotShardId: ShardId,
                              recoveryState: RecoveryState, listener: ActionListener<Void>) {
        launch(Dispatchers.IO + leaderClusterClient.threadPool().coroutineContext()) {
            store.incRef()
            restoreShardWithRetries(store, snapshotId, indexId, snapshotShardId,
                    recoveryState, listener, ::restoreShardUsingMultiChunkTransfer, log = log)
            // We will do decRef and releaseResources ultimately, not while during our retries/restarts of
            // restoreShard .
        }
    }

    suspend fun restoreShardUsingMultiChunkTransfer(store: Store, snapshotId: SnapshotId, indexId: IndexId,
                                                    snapshotShardId: ShardId,
                                                    recoveryState: RecoveryState, listener: ActionListener<Void>) {

        var multiChunkTransfer: RemoteClusterMultiChunkTransfer?
        var restoreUUID: String?
        var leaderShardNode: DiscoveryNode?
        var leaderShardId: ShardId?
        val followerIndexName = store.shardId().indexName
        val followerShardId = store.shardId()
        // 1. Get all the files info from the leader cluster for this shardId
        // Node containing the shard
        val leaderClusterState = getLeaderClusterState(true, true, indexId.name)
        val leaderShardRouting = leaderClusterState.routingTable.shardRoutingTable(snapshotShardId.indexName,
                snapshotShardId.id).primaryShard()
        leaderShardNode = leaderClusterState.nodes.get(leaderShardRouting.currentNodeId())
        // Get the index UUID of the leader cluster for the metadata request
        leaderShardId = ShardId(snapshotShardId.indexName,
                leaderClusterState.metadata.index(indexId.name).indexUUID,
                snapshotShardId.id)
        restoreUUID = UUIDs.randomBase64UUID()
        val getStoreMetadataRequest = GetStoreMetadataRequest(restoreUUID, leaderShardNode, leaderShardId,
            RemoteClusterRetentionLeaseHelper.getFollowerClusterNameWithUUID(clusterService.clusterName.value(), clusterService.state().metadata.clusterUUID()),
             followerShardId)

        // Gets the remote store metadata
        val metadataResponse = executeActionOnRemote(GetStoreMetadataAction.INSTANCE, getStoreMetadataRequest, followerIndexName)
        val metadataSnapshot = metadataResponse.metadataSnapshot

        val replMetadata = getReplicationMetadata(followerIndexName)
        // 2. Request for individual files from leader cluster for this shardId
        // make sure the store is not released until we are done.
        val fileMetadata = ArrayList(metadataSnapshot.asMap().values)
        multiChunkTransfer = RemoteClusterMultiChunkTransfer(log, clusterService.clusterName.value(), client.threadPool().threadContext,
                store, replicationSettings.concurrentFileChunks, restoreUUID, replMetadata, leaderShardNode,
                leaderShardId, fileMetadata, leaderClusterClient, recoveryState, replicationSettings.chunkSize,
                object : ActionListener<Void> {
                    override fun onFailure(e: java.lang.Exception?) {
                        log.error("Restore of ${store.shardId()} failed due to ${e?.stackTraceToString()}")
                        if (e is NodeDisconnectedException || e is NodeNotConnectedException || e is ConnectTransportException) {
                            log.info("Retrying restore shard for ${store.shardId()}")
                            Thread.sleep(1000) // to get updated leader cluster state
                            launch(Dispatchers.IO + leaderClusterClient.threadPool().coroutineContext()) {
                                restoreShardWithRetries(store, snapshotId, indexId, snapshotShardId,
                                        recoveryState, listener, ::restoreShardUsingMultiChunkTransfer, log = log)
                            }
                        } else {
                            log.error("Not retrying restore shard for ${store.shardId()}")
                            store.decRef()
                            releaseLeaderResources(restoreUUID, leaderShardNode, leaderShardId, followerShardId, followerIndexName)
                            listener.onFailure(e)
                        }

                    }

                    override fun onResponse(response: Void?) {
                        log.info("Restore successful for ${store.shardId()}")
                        store.decRef()
                        releaseLeaderResources(restoreUUID, leaderShardNode, leaderShardId, followerShardId, followerIndexName)
                        listener.onResponse(null)
                    }
                })
        if (fileMetadata.isEmpty()) {
            log.info("Initializing with empty store for shard:" + snapshotShardId.id)
            store.createEmpty(store.indexSettings().indexVersionCreated.luceneVersion)
            store.decRef()
            releaseLeaderResources(restoreUUID, leaderShardNode, leaderShardId, followerShardId, followerIndexName)
            listener.onResponse(null)
        } else {
            multiChunkTransfer.start()
        }
    }


    private fun releaseLeaderResources(restoreUUID: String, leaderShardNode: DiscoveryNode,
                                       leaderShardId: ShardId, followerShardId: ShardId, followerIndexName: String) {
        try {
            val releaseResourcesReq = ReleaseLeaderResourcesRequest(restoreUUID, leaderShardNode, leaderShardId,
                    clusterService.clusterName.value(), followerShardId)
            if (leaderClusterGetAction(ReleaseLeaderResourcesAction.INSTANCE, releaseResourcesReq, followerIndexName).isAcknowledged) {
                log.info("Successfully released resources at the leader cluster for $leaderShardId at $leaderShardNode")
            }
        } catch (e: Exception) {
            log.error("Releasing leader resource failed due to ${e.stackTraceToString()}")
        }

    }

    override fun isReadOnly(): Boolean {
        return true
    }

    override fun isSystemRepository(): Boolean {
        throw UnsupportedOperationException("Operation not permitted")
    }


    /*
     * This method makes a blocking call to the leader cluster
     * For restore workflow this is expected.
     */
    private fun getLeaderClusterState(includeNodes: Boolean, includeRoutingTable: Boolean, vararg remoteIndices: String): ClusterState {
        val clusterStateRequest = leaderClusterClient.admin().cluster().prepareState()
                .clear()
                .setIndices(*remoteIndices)
                .setMetadata(true)
                .setNodes(includeNodes)
                .setRoutingTable(includeRoutingTable)
                .setIndicesOptions(IndicesOptions.strictSingleIndexNoExpandForbidClosed())
                .request()

        val remoteState = leaderClusterClient.admin().cluster().state(clusterStateRequest)
                .actionGet(REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC).state
        log.trace("Successfully fetched the cluster state from remote repository ${remoteState}")
        return remoteState
    }


    private fun getReplicationMetadata(followerIndex: String): ReplicationMetadata {
        return replicationMetadataManager.getIndexReplicationMetadata(followerIndex,
                repositoryMetadata.leaderClusterName(), fetch_from_primary = true)
    }


    /*
    * Makes transport action to the leader cluster by making a blocking call
    * For restore workflow this is expected.
    */
    private fun <T : ActionResponse> leaderClusterGetAction(actionType: ActionType<T>,
                                                            actionRequest: ActionRequest,
                                                            followerIndex: String): T {

        val replMetadata = getReplicationMetadata(followerIndex)
        return leaderClusterClient.execute(replMetadata, actionType, actionRequest,
                REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC)

    }

    /*
    * Makes transport action to the leader cluster by making a non blocking call.
    */
    private suspend fun <T : ActionResponse> executeActionOnRemote(actionType: ActionType<T>,
                                                                   actionRequest: ActionRequest,
                                                                   followerIndex: String): T {

        val replMetadata = getReplicationMetadata(followerIndex)
        return leaderClusterClient.suspendExecute(replMetadata, actionType, actionRequest)

    }
}
