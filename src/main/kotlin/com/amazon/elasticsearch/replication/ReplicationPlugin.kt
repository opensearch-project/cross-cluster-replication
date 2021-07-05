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

package com.amazon.elasticsearch.replication

import com.amazon.elasticsearch.replication.action.autofollow.AutoFollowMasterNodeAction
import com.amazon.elasticsearch.replication.action.autofollow.TransportAutoFollowMasterNodeAction
import com.amazon.elasticsearch.replication.action.autofollow.TransportUpdateAutoFollowPatternAction
import com.amazon.elasticsearch.replication.action.autofollow.UpdateAutoFollowPatternAction
import com.amazon.elasticsearch.replication.action.changes.GetChangesAction
import com.amazon.elasticsearch.replication.action.changes.TransportGetChangesAction
import com.amazon.elasticsearch.replication.action.index.ReplicateIndexAction
import com.amazon.elasticsearch.replication.action.index.ReplicateIndexMasterNodeAction
import com.amazon.elasticsearch.replication.action.index.TransportReplicateIndexAction
import com.amazon.elasticsearch.replication.action.index.TransportReplicateIndexMasterNodeAction
import com.amazon.elasticsearch.replication.action.replay.ReplayChangesAction
import com.amazon.elasticsearch.replication.action.replay.TransportReplayChangesAction
import com.amazon.elasticsearch.replication.action.repository.GetFileChunkAction
import com.amazon.elasticsearch.replication.action.repository.GetStoreMetadataAction
import com.amazon.elasticsearch.replication.action.repository.ReleaseLeaderResourcesAction
import com.amazon.elasticsearch.replication.action.repository.TransportGetFileChunkAction
import com.amazon.elasticsearch.replication.action.repository.TransportGetStoreMetadataAction
import com.amazon.elasticsearch.replication.action.stop.StopIndexReplicationAction
import com.amazon.elasticsearch.replication.action.stop.TransportStopIndexReplicationAction
import com.amazon.elasticsearch.replication.action.repository.TransportReleaseLeaderResourcesAction
import com.amazon.elasticsearch.replication.metadata.state.ReplicationStateMetadata
import com.amazon.elasticsearch.replication.repository.REMOTE_REPOSITORY_TYPE
import com.amazon.elasticsearch.replication.repository.RemoteClusterRepositoriesService
import com.amazon.elasticsearch.replication.repository.RemoteClusterRepository
import com.amazon.elasticsearch.replication.repository.RemoteClusterRestoreLeaderService
import com.amazon.elasticsearch.replication.task.IndexCloseListener
import com.amazon.elasticsearch.replication.task.autofollow.AutoFollowExecutor
import com.amazon.elasticsearch.replication.task.autofollow.AutoFollowParams
import com.amazon.elasticsearch.replication.task.index.IndexReplicationExecutor
import com.amazon.elasticsearch.replication.task.index.IndexReplicationParams
import com.amazon.elasticsearch.replication.task.index.IndexReplicationState
import com.amazon.elasticsearch.replication.task.shard.ShardReplicationExecutor
import com.amazon.elasticsearch.replication.task.shard.ShardReplicationParams
import com.amazon.elasticsearch.replication.task.shard.ShardReplicationState
import com.amazon.elasticsearch.replication.util.Injectables
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.NamedDiff
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.metadata.Metadata
import org.elasticsearch.cluster.metadata.RepositoryMetadata
import org.elasticsearch.cluster.node.DiscoveryNodes
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.CheckedFunction
import org.elasticsearch.common.ParseField
import org.elasticsearch.common.component.LifecycleComponent
import org.elasticsearch.common.io.stream.NamedWriteableRegistry
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.settings.ClusterSettings
import org.elasticsearch.common.settings.IndexScopedSettings
import org.elasticsearch.common.settings.Setting
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.settings.SettingsFilter
import org.elasticsearch.common.settings.SettingsModule
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.env.Environment
import org.elasticsearch.env.NodeEnvironment
import org.elasticsearch.index.IndexModule
import org.elasticsearch.index.IndexSettings
import org.elasticsearch.index.engine.EngineFactory
import org.elasticsearch.indices.recovery.RecoverySettings
import org.elasticsearch.persistent.PersistentTaskParams
import org.elasticsearch.persistent.PersistentTaskState
import org.elasticsearch.persistent.PersistentTasksExecutor
import org.elasticsearch.plugins.ActionPlugin
import org.elasticsearch.plugins.ActionPlugin.ActionHandler
import org.elasticsearch.plugins.EnginePlugin
import org.elasticsearch.plugins.PersistentTaskPlugin
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.plugins.RepositoryPlugin
import org.elasticsearch.repositories.RepositoriesService
import org.elasticsearch.repositories.Repository
import org.elasticsearch.rest.RestController
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.script.ScriptService
import org.elasticsearch.threadpool.ExecutorBuilder
import org.elasticsearch.threadpool.ScalingExecutorBuilder
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.watcher.ResourceWatcherService
import java.util.Optional
import java.util.function.Supplier
import com.amazon.elasticsearch.replication.action.index.block.UpdateIndexBlockAction
import com.amazon.elasticsearch.replication.action.index.block.TransportUpddateIndexBlockAction
import com.amazon.elasticsearch.replication.action.pause.PauseIndexReplicationAction
import com.amazon.elasticsearch.replication.action.pause.TransportPauseIndexReplicationAction
import com.amazon.elasticsearch.replication.action.replicationstatedetails.TransportUpdateReplicationStateDetails
import com.amazon.elasticsearch.replication.action.replicationstatedetails.UpdateReplicationStateAction
import com.amazon.elasticsearch.replication.action.resume.ResumeIndexReplicationAction
import com.amazon.elasticsearch.replication.action.resume.TransportResumeIndexReplicationAction
import com.amazon.elasticsearch.replication.metadata.TransportUpdateMetadataAction
import com.amazon.elasticsearch.replication.metadata.UpdateMetadataAction
import com.amazon.elasticsearch.replication.rest.PauseIndexReplicationHandler
import com.amazon.elasticsearch.replication.rest.ReplicateIndexHandler
import com.amazon.elasticsearch.replication.rest.ResumeIndexReplicationHandler
import com.amazon.elasticsearch.replication.rest.StopIndexReplicationHandler
import com.amazon.elasticsearch.replication.rest.UpdateAutoFollowPatternsHandler
import org.elasticsearch.common.util.concurrent.EsExecutors
import org.elasticsearch.monitor.jvm.JvmInfo
import org.elasticsearch.threadpool.FixedExecutorBuilder
import com.amazon.elasticsearch.replication.action.setup.SetupChecksAction
import com.amazon.elasticsearch.replication.action.setup.TransportSetupChecksAction
import com.amazon.elasticsearch.replication.action.setup.TransportValidatePermissionsAction
import com.amazon.elasticsearch.replication.action.setup.ValidatePermissionsAction
import com.amazon.elasticsearch.replication.action.status.ShardsInfoAction
import com.amazon.elasticsearch.replication.action.status.ReplicationStatusAction
import com.amazon.elasticsearch.replication.action.status.TranportShardsInfoAction
import com.amazon.elasticsearch.replication.action.status.TransportReplicationStatusAction
import com.amazon.elasticsearch.replication.metadata.ReplicationMetadataManager
import com.amazon.elasticsearch.replication.metadata.store.ReplicationMetadataStore
import com.amazon.elasticsearch.replication.rest.*
import com.amazon.elasticsearch.replication.seqno.RemoteClusterTranslogService
import com.amazon.elasticsearch.replication.action.update.TransportUpdateIndexReplicationAction
import com.amazon.elasticsearch.replication.action.update.UpdateIndexReplicationAction
import com.amazon.elasticsearch.replication.rest.UpdateIndexHandler
import kotlin.math.min

internal class ReplicationPlugin : Plugin(), ActionPlugin, PersistentTaskPlugin, RepositoryPlugin, EnginePlugin {

    private lateinit var client: Client
    private lateinit var threadPool: ThreadPool
    private lateinit var replicationMetadataManager: ReplicationMetadataManager

    companion object {
        const val MAX_TRANSLOG_BUFFER_PERCENT = 50
        const val REPLICATION_EXECUTOR_NAME_LEADER = "replication_leader"
        const val REPLICATION_EXECUTOR_NAME_FOLLOWER = "replication_follower"
        val REPLICATED_INDEX_SETTING: Setting<String> = Setting.simpleString("index.plugins.replication.replicated",
            Setting.Property.InternalIndex, Setting.Property.IndexScope)
        val REPLICATION_CHANGE_BATCH_SIZE: Setting<Int> = Setting.intSetting("plugins.replication.ops_batch_size", 512, 16,
            Setting.Property.Dynamic, Setting.Property.NodeScope)
        val REPLICATION_LEADER_THREADPOOL_SIZE: Setting<Int> = Setting.intSetting("plugins.replication.leader.size", 0, 0,
            Setting.Property.Dynamic, Setting.Property.NodeScope)
        val REPLICATION_LEADER_THREADPOOL_QUEUE_SIZE: Setting<Int> = Setting.intSetting("plugins.replication.leader.queue_size", 1000, 0,
            Setting.Property.Dynamic, Setting.Property.NodeScope)
        val REPLICATION_TRANSLOG_BUFFER_PERCENT: Setting<Int> = Setting.intSetting(
                "opendistro.replication.translog_buffer_percent", 20, 1,
                Setting.Property.Dynamic, Setting.Property.NodeScope)
    }

    override fun createComponents(client: Client, clusterService: ClusterService, threadPool: ThreadPool,
                                  resourceWatcherService: ResourceWatcherService, scriptService: ScriptService,
                                  xContentRegistry: NamedXContentRegistry, environment: Environment,
                                  nodeEnvironment: NodeEnvironment,
                                  namedWriteableRegistry: NamedWriteableRegistry,
                                  indexNameExpressionResolver: IndexNameExpressionResolver,
                                  repositoriesService: Supplier<RepositoriesService>): Collection<Any> {
        this.client = client
        this.threadPool = threadPool
        this.replicationMetadataManager = ReplicationMetadataManager(clusterService, client,
                ReplicationMetadataStore(client, clusterService, xContentRegistry))
        return listOf(RemoteClusterRepositoriesService(repositoriesService, clusterService), replicationMetadataManager)
    }

    override fun getGuiceServiceClasses(): Collection<Class<out LifecycleComponent>> {
        return listOf(Injectables::class.java,
                RemoteClusterRestoreLeaderService::class.java, RemoteClusterTranslogService::class.java)
    }

    override fun getActions(): List<ActionHandler<out ActionRequest, out ActionResponse>> {
        return listOf(ActionHandler(GetChangesAction.INSTANCE, TransportGetChangesAction::class.java),
            ActionHandler(ReplicateIndexAction.INSTANCE, TransportReplicateIndexAction::class.java),
            ActionHandler(ReplicateIndexMasterNodeAction.INSTANCE, TransportReplicateIndexMasterNodeAction::class.java),
            ActionHandler(ReplayChangesAction.INSTANCE, TransportReplayChangesAction::class.java),
            ActionHandler(GetStoreMetadataAction.INSTANCE, TransportGetStoreMetadataAction::class.java),
            ActionHandler(GetFileChunkAction.INSTANCE, TransportGetFileChunkAction::class.java),
            ActionHandler(UpdateAutoFollowPatternAction.INSTANCE, TransportUpdateAutoFollowPatternAction::class.java),
            ActionHandler(AutoFollowMasterNodeAction.INSTANCE, TransportAutoFollowMasterNodeAction::class.java),
            ActionHandler(StopIndexReplicationAction.INSTANCE, TransportStopIndexReplicationAction::class.java),
            ActionHandler(PauseIndexReplicationAction.INSTANCE, TransportPauseIndexReplicationAction::class.java),
            ActionHandler(ResumeIndexReplicationAction.INSTANCE, TransportResumeIndexReplicationAction::class.java),
            ActionHandler(UpdateIndexReplicationAction.INSTANCE, TransportUpdateIndexReplicationAction::class.java),
            ActionHandler(UpdateIndexBlockAction.INSTANCE, TransportUpddateIndexBlockAction::class.java),
            ActionHandler(ReleaseLeaderResourcesAction.INSTANCE, TransportReleaseLeaderResourcesAction::class.java),
            ActionHandler(UpdateMetadataAction.INSTANCE, TransportUpdateMetadataAction::class.java),
            ActionHandler(ValidatePermissionsAction.INSTANCE, TransportValidatePermissionsAction::class.java),
            ActionHandler(SetupChecksAction.INSTANCE, TransportSetupChecksAction::class.java),
            ActionHandler(UpdateReplicationStateAction.INSTANCE, TransportUpdateReplicationStateDetails::class.java),
            ActionHandler(ShardsInfoAction.INSTANCE, TranportShardsInfoAction::class.java),
            ActionHandler(ReplicationStatusAction.INSTANCE,TransportReplicationStatusAction::class.java)
        )
    }

    override fun getRestHandlers(settings: Settings?, restController: RestController,
                                 clusterSettings: ClusterSettings?, indexScopedSettings: IndexScopedSettings,
                                 settingsFilter: SettingsFilter?,
                                 indexNameExpressionResolver: IndexNameExpressionResolver,
                                 nodesInCluster: Supplier<DiscoveryNodes>): List<RestHandler> {
        return listOf(ReplicateIndexHandler(),
            UpdateAutoFollowPatternsHandler(),
            PauseIndexReplicationHandler(),
            ResumeIndexReplicationHandler(),
            UpdateIndexHandler(),
            StopIndexReplicationHandler(),
            ReplicationStatusHandler())
    }

    override fun getExecutorBuilders(settings: Settings): List<ExecutorBuilder<*>> {
        return listOf(followerExecutorBuilder(settings), leaderExecutorBuilder(settings))
    }

    private fun followerExecutorBuilder(settings: Settings): ExecutorBuilder<*> {
        return ScalingExecutorBuilder(REPLICATION_EXECUTOR_NAME_FOLLOWER, 1, 10, TimeValue.timeValueMinutes(1), REPLICATION_EXECUTOR_NAME_FOLLOWER)
    }

    /**
     * Keeping the default configuration for threadpool in parity with search threadpool which is what we were using earlier.
     * https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/threadpool/ThreadPool.java#L195
     */
    private fun leaderExecutorBuilder(settings: Settings): ExecutorBuilder<*> {
        val availableProcessors = EsExecutors.allocatedProcessors(settings)
        val leaderThreadPoolQueueSize = REPLICATION_LEADER_THREADPOOL_QUEUE_SIZE.get(settings)

        var leaderThreadPoolSize = REPLICATION_LEADER_THREADPOOL_SIZE.get(settings)
        leaderThreadPoolSize = if (leaderThreadPoolSize > 0) leaderThreadPoolSize else leaderThreadPoolSize(availableProcessors)

        return FixedExecutorBuilder(settings, REPLICATION_EXECUTOR_NAME_LEADER, leaderThreadPoolSize, leaderThreadPoolQueueSize, REPLICATION_EXECUTOR_NAME_LEADER)
    }
    fun leaderThreadPoolSize(allocatedProcessors: Int): Int {
        return allocatedProcessors * 3 / 2 + 1
    }

    override fun getPersistentTasksExecutor(clusterService: ClusterService, threadPool: ThreadPool, client: Client,
                                            settingsModule: SettingsModule,
                                            expressionResolver: IndexNameExpressionResolver)
        : List<PersistentTasksExecutor<*>> {

        val bufferSize = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() *
                min(clusterService.clusterSettings.get(REPLICATION_TRANSLOG_BUFFER_PERCENT), MAX_TRANSLOG_BUFFER_PERCENT)/ 100
        val translogBuffer = TranslogBuffer(bufferSize)
        return listOf(
            ShardReplicationExecutor(REPLICATION_EXECUTOR_NAME_FOLLOWER, clusterService, threadPool, client, replicationMetadataManager, translogBuffer),
            IndexReplicationExecutor(REPLICATION_EXECUTOR_NAME_FOLLOWER, clusterService, threadPool, client, replicationMetadataManager),
            AutoFollowExecutor(REPLICATION_EXECUTOR_NAME_FOLLOWER, clusterService, threadPool, client, replicationMetadataManager))
    }

    override fun getNamedWriteables(): List<NamedWriteableRegistry.Entry> {
        return listOf(
            NamedWriteableRegistry.Entry(PersistentTaskParams::class.java, ShardReplicationParams.NAME,
            // can't directly pass in ::ReplicationTaskParams due to https://youtrack.jetbrains.com/issue/KT-35912
            Writeable.Reader { inp -> ShardReplicationParams(inp) }),
            NamedWriteableRegistry.Entry(PersistentTaskState::class.java, ShardReplicationState.NAME,
            Writeable.Reader { inp -> ShardReplicationState.reader(inp) }),

            NamedWriteableRegistry.Entry(PersistentTaskParams::class.java, IndexReplicationParams.NAME,
                Writeable.Reader { inp -> IndexReplicationParams(inp) }),
            NamedWriteableRegistry.Entry(PersistentTaskState::class.java, IndexReplicationState.NAME,
                Writeable.Reader { inp -> IndexReplicationState.reader(inp) }),

            NamedWriteableRegistry.Entry(PersistentTaskParams::class.java, AutoFollowParams.NAME,
                                         Writeable.Reader { inp -> AutoFollowParams(inp) }),

            NamedWriteableRegistry.Entry(Metadata.Custom::class.java, ReplicationStateMetadata.NAME,
                Writeable.Reader { inp -> ReplicationStateMetadata(inp) }),
            NamedWriteableRegistry.Entry(NamedDiff::class.java, ReplicationStateMetadata.NAME,
                Writeable.Reader { inp -> ReplicationStateMetadata.Diff(inp) })

        )
    }

    override fun getNamedXContent(): List<NamedXContentRegistry.Entry> {
        return listOf(
            NamedXContentRegistry.Entry(PersistentTaskParams::class.java,
                    ParseField(IndexReplicationParams.NAME),
                    CheckedFunction { parser: XContentParser -> IndexReplicationParams.fromXContent(parser)}),
            NamedXContentRegistry.Entry(PersistentTaskState::class.java,
                    ParseField(IndexReplicationState.NAME),
                    CheckedFunction { parser: XContentParser -> IndexReplicationState.fromXContent(parser)}),
            NamedXContentRegistry.Entry(PersistentTaskParams::class.java,
                    ParseField(ShardReplicationParams.NAME),
                    CheckedFunction { parser: XContentParser -> ShardReplicationParams.fromXContent(parser)}),
            NamedXContentRegistry.Entry(PersistentTaskState::class.java,
                    ParseField(ShardReplicationState.NAME),
                    CheckedFunction { parser: XContentParser -> ShardReplicationState.fromXContent(parser)}),
            NamedXContentRegistry.Entry(PersistentTaskParams::class.java,
                    ParseField(AutoFollowParams.NAME),
                    CheckedFunction { parser: XContentParser -> AutoFollowParams.fromXContent(parser)}),
            NamedXContentRegistry.Entry(Metadata.Custom::class.java,
                    ParseField(ReplicationStateMetadata.NAME),
                    CheckedFunction { parser: XContentParser -> ReplicationStateMetadata.fromXContent(parser)})
        )
    }

    override fun getSettings(): List<Setting<*>> {
        return listOf(REPLICATED_INDEX_SETTING,
                REPLICATION_CHANGE_BATCH_SIZE,
                REPLICATION_LEADER_THREADPOOL_SIZE,
                REPLICATION_LEADER_THREADPOOL_QUEUE_SIZE,
                REPLICATION_TRANSLOG_BUFFER_PERCENT)
    }

    override fun getInternalRepositories(env: Environment, namedXContentRegistry: NamedXContentRegistry,
                                         clusterService: ClusterService, recoverySettings: RecoverySettings): Map<String, Repository.Factory> {
        val repoFactory = Repository.Factory { repoMetadata: RepositoryMetadata ->
            RemoteClusterRepository(repoMetadata, client, clusterService, recoverySettings, replicationMetadataManager) }
        return mapOf(REMOTE_REPOSITORY_TYPE to repoFactory)
    }

    override fun getEngineFactory(indexSettings: IndexSettings): Optional<EngineFactory> {
        return if (indexSettings.settings.get(REPLICATED_INDEX_SETTING.key) != null) {
            Optional.of(EngineFactory { config -> ReplicationEngine(config) })
        } else {
            Optional.empty()
        }
    }

    override fun onIndexModule(indexModule: IndexModule) {
        super.onIndexModule(indexModule)
        if (indexModule.settings.get(REPLICATED_INDEX_SETTING.key) != null) {
            indexModule.addIndexEventListener(IndexCloseListener)
        }
    }
}
