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
import com.amazon.elasticsearch.replication.metadata.ReplicationMetadata
import com.amazon.elasticsearch.replication.repository.REMOTE_REPOSITORY_TYPE
import com.amazon.elasticsearch.replication.repository.RemoteClusterRepositoriesService
import com.amazon.elasticsearch.replication.repository.RemoteClusterRepository
import com.amazon.elasticsearch.replication.repository.RemoteClusterRestoreLeaderService
import com.amazon.elasticsearch.replication.rest.ReplicateIndexHandler
import com.amazon.elasticsearch.replication.rest.StopIndexReplicationHandler
import com.amazon.elasticsearch.replication.rest.UpdateAutoFollowPatternsHandler
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
import com.amazon.elasticsearch.replication.action.status.IndexReplicationStatusAction
import com.amazon.elasticsearch.replication.action.status.TransportIndexReplicationStatusAction
import com.amazon.elasticsearch.replication.rest.ReplicationStatusHandler

internal class ReplicationPlugin : Plugin(), ActionPlugin, PersistentTaskPlugin, RepositoryPlugin, EnginePlugin {

    private lateinit var client: Client
    private lateinit var threadPool: ThreadPool

    companion object {
        const val REPLICATION_EXECUTOR_NAME = "replication"
        val REPLICATED_INDEX_SETTING = Setting.simpleString("index.opendistro.replicated",
            Setting.Property.InternalIndex, Setting.Property.IndexScope)
        val REPLICATION_CHANGE_BATCH_SIZE = Setting.intSetting("opendistro.replication.ops_batch_size", 512, 16,
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
        return listOf(RemoteClusterRepositoriesService(repositoriesService, clusterService))
    }

    override fun getGuiceServiceClasses(): Collection<Class<out LifecycleComponent>> {
        return listOf(Injectables::class.java, RemoteClusterRestoreLeaderService::class.java)
    }

    override fun getActions(): List<ActionHandler<out ActionRequest, out ActionResponse>> {
        return listOf(ActionHandler(GetChangesAction.INSTANCE, TransportGetChangesAction::class.java),
            ActionHandler(ReplicateIndexAction.INSTANCE, TransportReplicateIndexAction::class.java),
            ActionHandler(ReplicateIndexMasterNodeAction.INSTANCE, TransportReplicateIndexMasterNodeAction::class.java),
            ActionHandler(ReplayChangesAction.INSTANCE, TransportReplayChangesAction::class.java),
            ActionHandler(GetStoreMetadataAction.INSTANCE, TransportGetStoreMetadataAction::class.java),
            ActionHandler(GetFileChunkAction.INSTANCE, TransportGetFileChunkAction::class.java),
            ActionHandler(UpdateAutoFollowPatternAction.INSTANCE, TransportUpdateAutoFollowPatternAction::class.java),
            ActionHandler(StopIndexReplicationAction.INSTANCE, TransportStopIndexReplicationAction::class.java),
            ActionHandler(UpdateIndexBlockAction.INSTANCE, TransportUpddateIndexBlockAction::class.java),
            ActionHandler(ReleaseLeaderResourcesAction.INSTANCE, TransportReleaseLeaderResourcesAction::class.java),
            ActionHandler(IndexReplicationStatusAction.INSTANCE, TransportIndexReplicationStatusAction::class.java)
        )
    }

    override fun getRestHandlers(settings: Settings?, restController: RestController,
                                 clusterSettings: ClusterSettings?, indexScopedSettings: IndexScopedSettings,
                                 settingsFilter: SettingsFilter?,
                                 indexNameExpressionResolver: IndexNameExpressionResolver,
                                 nodesInCluster: Supplier<DiscoveryNodes>): List<RestHandler> {
        return listOf(ReplicateIndexHandler(),
            UpdateAutoFollowPatternsHandler(),
            StopIndexReplicationHandler(),
            ReplicationStatusHandler())
    }

    override fun getExecutorBuilders(settings: Settings): List<ExecutorBuilder<*>> {
        //TODO: get the executor size from settings
        return listOf(ScalingExecutorBuilder(REPLICATION_EXECUTOR_NAME, 1, 10, TimeValue.timeValueMinutes(1)))
    }

    override fun getPersistentTasksExecutor(clusterService: ClusterService, threadPool: ThreadPool, client: Client,
                                            settingsModule: SettingsModule,
                                            expressionResolver: IndexNameExpressionResolver)
        : List<PersistentTasksExecutor<*>> {
        return listOf(
            ShardReplicationExecutor(REPLICATION_EXECUTOR_NAME, clusterService, threadPool, client),
            IndexReplicationExecutor(REPLICATION_EXECUTOR_NAME, clusterService, threadPool, client),
            AutoFollowExecutor(REPLICATION_EXECUTOR_NAME, clusterService, threadPool, client))
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

            NamedWriteableRegistry.Entry(Metadata.Custom::class.java, ReplicationMetadata.NAME,
                Writeable.Reader { inp -> ReplicationMetadata(inp) }),
            NamedWriteableRegistry.Entry(NamedDiff::class.java, ReplicationMetadata.NAME,
                Writeable.Reader { inp -> ReplicationMetadata.Diff(inp) })

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
                    ParseField(ReplicationMetadata.NAME),
                    CheckedFunction { parser: XContentParser -> ReplicationMetadata.fromXContent(parser)})
        )
    }

    override fun getSettings(): List<Setting<*>> {
        return listOf(REPLICATED_INDEX_SETTING, REPLICATION_CHANGE_BATCH_SIZE)
    }

    override fun getInternalRepositories(env: Environment, namedXContentRegistry: NamedXContentRegistry,
                                         clusterService: ClusterService, recoverySettings: RecoverySettings): Map<String, Repository.Factory> {
        val repoFactory = Repository.Factory { repoMetadata: RepositoryMetadata ->
            RemoteClusterRepository(repoMetadata, client, clusterService, recoverySettings) }
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
