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

package org.opensearch.replication.repository

import org.opensearch.replication.ReplicationPlugin
import org.opensearch.replication.action.repository.GetStoreMetadataAction
import org.opensearch.replication.action.repository.GetStoreMetadataRequest
import org.opensearch.replication.action.repository.ReleaseLeaderResourcesAction
import org.opensearch.replication.util.SecurityContext
import org.opensearch.replication.action.repository.ReleaseLeaderResourcesRequest
import org.apache.logging.log4j.LogManager
import org.apache.lucene.index.IndexCommit
import org.opensearch.Version
import org.opensearch.action.ActionListener
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionResponse
import org.opensearch.action.ActionType
import org.opensearch.action.admin.indices.stats.IndicesStatsAction
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.support.IndicesOptions
import org.opensearch.client.Client
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.ClusterStateUpdateTask
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.metadata.RepositoryMetadata
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.Nullable
import org.opensearch.common.UUIDs
import org.opensearch.common.component.AbstractLifecycleComponent
import org.opensearch.common.metrics.CounterMetric
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.index.mapper.MapperService
import org.opensearch.index.shard.ShardId
import org.opensearch.index.snapshots.IndexShardSnapshotStatus
import org.opensearch.index.store.Store
import org.opensearch.index.store.StoreStats
import org.opensearch.indices.recovery.RecoverySettings
import org.opensearch.indices.recovery.RecoveryState
import org.opensearch.replication.util.executeUnderSecurityContext
import org.opensearch.repositories.IndexId
import org.opensearch.repositories.Repository
import org.opensearch.repositories.RepositoryData
import org.opensearch.repositories.RepositoryShardId
import org.opensearch.repositories.ShardGenerations
import org.opensearch.snapshots.SnapshotId
import org.opensearch.snapshots.SnapshotInfo
import org.opensearch.snapshots.SnapshotState
import java.util.UUID
import java.util.function.Consumer
import java.util.function.Function
import kotlin.collections.ArrayList

const val REMOTE_REPOSITORY_PREFIX = "opendistro-remote-repo-"
const val REMOTE_REPOSITORY_TYPE = "opendistro-remote-repository"
const val REMOTE_SNAPSHOT_NAME = "opendistro-remote-snapshot"

class RemoteClusterRepository(private val repositoryMetadata: RepositoryMetadata,
                              private val client: Client,
                              private val clusterService: ClusterService,
                              private val recoverySettings: RecoverySettings): AbstractLifecycleComponent(), Repository {

    // Lazy init because we initialize when a remote cluster seed setting is added at which point the remote
    // cluster connection might not be available yet
    private val remoteClusterClient by lazy { client.getRemoteClusterClient(repositoryMetadata.remoteClusterName()) }

    companion object {
        private val log = LogManager.getLogger(RemoteClusterRepository::class.java)
        private val restoreRateLimitingTimeInNanos = CounterMetric()
        private fun String.asUUID() : String = UUID.nameUUIDFromBytes(toByteArray()).toString()
        private fun RepositoryMetadata.remoteClusterName() : String = this.name().split(REMOTE_REPOSITORY_PREFIX)[1]
        const val REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC = 60000L

        fun clusterForRepo(remoteRepoName: String) = remoteRepoName.split(REMOTE_REPOSITORY_PREFIX)[1]
        fun repoForCluster(remoteClusterName: String) : String = REMOTE_REPOSITORY_PREFIX + remoteClusterName
    }

    @Volatile private var parallelChunks = recoverySettings.maxConcurrentFileChunks
    @Volatile private var chunkSize = recoverySettings.chunkSize

    override fun getRestoreThrottleTimeInNanos(): Long {
        return restoreRateLimitingTimeInNanos.count()
    }

    override fun finalizeSnapshot(shardGenerations: ShardGenerations?, repositoryStateId: Long, clusterMetadata: Metadata?,
                                  snapshotInfo: SnapshotInfo?, repositoryMetaVersion: Version?,
                                  stateTransformer: Function<ClusterState, ClusterState>?,
                                  listener: ActionListener<RepositoryData>?) {
        throw UnsupportedOperationException("Operation not permitted")
    }

    override fun deleteSnapshots(snapshotIds: MutableCollection<SnapshotId>?, repositoryStateId: Long,
                                 repositoryMetaVersion: Version?, listener: ActionListener<RepositoryData>?) {
        throw UnsupportedOperationException("Operation not permitted")
    }

    override fun initializeSnapshot(snapshotId: SnapshotId, indices: MutableList<IndexId>, metadata: Metadata) {
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
        val indicesStatsResponse = remoteClusterGetAction(IndicesStatsAction.INSTANCE, indicesStatsRequest, shardId.indexName)
        for(i in indicesStatsResponse.shards.indices) {
            if(indicesStatsResponse.shards[i].shardRouting.shardId().id == shardId.id) {
                val sizeInBytes = indicesStatsResponse.shards[i].stats?.store?.sizeInBytes!!
                // Filling in dummy values except size
                return IndexShardSnapshotStatus.newDone(0L, 3L, 1,
                        1, sizeInBytes, sizeInBytes , "")
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
     * Step 1: Gets all the indices from the remote cluster.
     * At this point, we don't have information on targeted index for restore.
     * Fetches all the information and creates a repository data object for the restore workflow.
     */
    override fun getRepositoryData(listener: ActionListener<RepositoryData>) {
        val clusterState = getRemoteClusterState(false, false)
        val shardGenerations = ShardGenerations.builder()
        clusterState.metadata.indices.values()
            .map { it.value }
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
        val remoteClusterState = getRemoteClusterState(false, false)
        assert(REMOTE_SNAPSHOT_NAME.equals(snapshotId.name), { "SnapshotName differs" })
        val indices = remoteClusterState.metadata().indices().keys().map { x -> x.value }
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
        val remoteClusterState = getRemoteClusterState(false, false, index.name)
        val indexMetadata = remoteClusterState.metadata.index(index.name)

        // Add replication specific settings
        val builder = Settings.builder().put(indexMetadata.settings)
        val replicatedIndex = "${repositoryMetadata.remoteClusterName()}:${index.name}"
        builder.put(ReplicationPlugin.REPLICATED_INDEX_SETTING.key, replicatedIndex)
        val indexMdBuilder = IndexMetadata.builder(indexMetadata).settings(builder)
        indexMetadata.aliases.valuesIt().forEach {
            indexMdBuilder.putAlias(it)
        }
        return indexMdBuilder.build()
    }

    /*
     * Step 5: restore shard by fetching the lucene segments from the remote cluster
     */
    override fun restoreShard(store: Store, snapshotId: SnapshotId, indexId: IndexId, snapshotShardId: ShardId,
                              recoveryState: RecoveryState, listener: ActionListener<Void>) {
        var multiChunkTransfer: RemoteClusterMultiChunkTransfer?
        var restoreUUID: String?
        var remoteShardNode: DiscoveryNode?
        var remoteShardId: ShardId?
        try {
            store.incRef()
            val followerIndexName = store.shardId().indexName
            val followerShardId = store.shardId()
            // 1. Get all the files info from the remote cluster for this shardId
            // Node containing the shard
            val remoteClusterState = getRemoteClusterState(true, true, indexId.name)
            val remoteShardRouting = remoteClusterState.routingTable.shardRoutingTable(snapshotShardId.indexName,
                    snapshotShardId.id).primaryShard()
            remoteShardNode = remoteClusterState.nodes.get(remoteShardRouting.currentNodeId())

            // Get the index UUID of the remote cluster for the metadata request
            remoteShardId = ShardId(snapshotShardId.indexName,
                    remoteClusterState.metadata.index(indexId.name).indexUUID,
                    snapshotShardId.id)
            restoreUUID = UUIDs.randomBase64UUID()
            val getStoreMetadataRequest = GetStoreMetadataRequest(restoreUUID, remoteShardNode, remoteShardId,
                    clusterService.clusterName.value(), followerShardId)

            // Gets the remote store metadata
            val metadataResponse = remoteClusterGetAction(GetStoreMetadataAction.INSTANCE, getStoreMetadataRequest, followerIndexName)
            val metadataSnapshot = metadataResponse.metadataSnapshot

            // 2. Request for individual files from remote cluster for this shardId
            // make sure the store is not released until we are done.
            val fileMetadata = ArrayList(metadataSnapshot.asMap().values)
            multiChunkTransfer = RemoteClusterMultiChunkTransfer(log, clusterService.clusterName.value(), client.threadPool().threadContext,
                    store, parallelChunks, restoreUUID, remoteShardNode,
                    remoteShardId, fileMetadata, remoteClusterClient, recoveryState, chunkSize,
                    object: ActionListener<Void>{
                        override fun onFailure(e: java.lang.Exception?) {
                            log.error("Restore of ${store.shardId()} failed due to $e")
                            store.decRef()
                            releaseLeaderResources(restoreUUID, remoteShardNode, remoteShardId, followerShardId, followerIndexName)
                            listener.onFailure(e)
                        }
                        override fun onResponse(response: Void?) {
                            log.info("Restore successful for ${store.shardId()}")
                            store.decRef()
                            releaseLeaderResources(restoreUUID, remoteShardNode, remoteShardId, followerShardId, followerIndexName)
                            listener.onResponse(null)
                        }
                    })
            if(fileMetadata.isEmpty()) {
                log.info("Initializing with empty store for shard:" + snapshotShardId.id)
                store.createEmpty(store.indexSettings().indexVersionCreated.luceneVersion)
                store.decRef()
                releaseLeaderResources(restoreUUID, remoteShardNode, remoteShardId, followerShardId, followerIndexName)
                listener.onResponse(null)
            }
            else {
                remoteClusterClient.executeUnderSecurityContext(clusterService, repositoryMetadata.remoteClusterName(), followerIndexName) {
                    multiChunkTransfer.start()
                }
            }
        } catch (e: Exception) {
            log.error("Restore of shard from remote cluster repository failed due to $e")
            store.decRef()
            listener.onFailure(e)
        }
    }

    private fun releaseLeaderResources(restoreUUID: String, remoteShardNode: DiscoveryNode,
                                       remoteShardId: ShardId, followerShardId: ShardId, followerIndexName: String) {
        val releaseResourcesReq = ReleaseLeaderResourcesRequest(restoreUUID, remoteShardNode, remoteShardId,
                clusterService.clusterName.value(), followerShardId)
        if(remoteClusterGetAction(ReleaseLeaderResourcesAction.INSTANCE, releaseResourcesReq, followerIndexName).isAcknowledged) {
            log.info("Successfully released resources at the leader cluster for $remoteShardId at $remoteShardNode")
        }
    }

    override fun isReadOnly(): Boolean {
        return true
    }

    /*
     * This method makes a blocking call to the remote cluster
     * For restore workflow this is expected.
     */
    private fun getRemoteClusterState(includeNodes: Boolean, includeRoutingTable: Boolean, vararg remoteIndices: String): ClusterState {
        val clusterStateRequest = remoteClusterClient.admin().cluster().prepareState()
                .clear()
                .setIndices(*remoteIndices)
                .setMetadata(true)
                .setNodes(includeNodes)
                .setRoutingTable(includeRoutingTable)
                .setIndicesOptions(IndicesOptions.strictSingleIndexNoExpandForbidClosed())
                .request()

        val remoteState = remoteClusterClient.admin().cluster().state(clusterStateRequest)
                .actionGet(REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC).state
        log.trace("Successfully fetched the cluster state from remote repository ${remoteState}")
        return remoteState
    }

    /*
    * Makes transport action to the remote cluster by making a blocking call
    * For restore workflow this is expected.
    */
    private fun <T : ActionResponse> remoteClusterGetAction(actionType: ActionType<T>,
                                                            actionRequest: ActionRequest,
                                                            followerIndex: String): T {
        val userString = SecurityContext.fromClusterState(clusterService.state(),
                                                        repositoryMetadata.remoteClusterName(),
                                                        followerIndex)
        var storedContext: ThreadContext.StoredContext? = null
        try {
            storedContext = remoteClusterClient.threadPool().threadContext.newStoredContext(true)
            SecurityContext.toThreadContext(remoteClusterClient.threadPool().threadContext, userString)
            return remoteClusterClient.execute(actionType, actionRequest).actionGet(REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC)
        }
        finally {
            storedContext?.restore()
        }
    }
}
