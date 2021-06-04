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

package org.opensearch.replication

import org.opensearch.replication.action.autofollow.TransportUpdateAutoFollowPatternAction
import org.opensearch.replication.action.autofollow.UpdateAutoFollowPatternAction
import org.opensearch.replication.action.changes.GetChangesAction
import org.opensearch.replication.action.changes.TransportGetChangesAction
import org.opensearch.replication.action.index.ReplicateIndexAction
import org.opensearch.replication.action.index.ReplicateIndexMasterNodeAction
import org.opensearch.replication.action.index.TransportReplicateIndexAction
import org.opensearch.replication.action.index.TransportReplicateIndexMasterNodeAction
import org.opensearch.replication.action.replay.ReplayChangesAction
import org.opensearch.replication.action.replay.TransportReplayChangesAction
import org.opensearch.replication.action.repository.GetFileChunkAction
import org.opensearch.replication.action.repository.GetStoreMetadataAction
import org.opensearch.replication.action.repository.ReleaseLeaderResourcesAction
import org.opensearch.replication.action.repository.TransportGetFileChunkAction
import org.opensearch.replication.action.repository.TransportGetStoreMetadataAction
import org.opensearch.replication.action.stop.StopIndexReplicationAction
import org.opensearch.replication.action.stop.TransportStopIndexReplicationAction
import org.opensearch.replication.action.repository.TransportReleaseLeaderResourcesAction
import org.opensearch.replication.metadata.ReplicationMetadata
import org.opensearch.replication.repository.REMOTE_REPOSITORY_TYPE
import org.opensearch.replication.repository.RemoteClusterRepositoriesService
import org.opensearch.replication.repository.RemoteClusterRepository
import org.opensearch.replication.repository.RemoteClusterRestoreLeaderService
import org.opensearch.replication.rest.ReplicateIndexHandler
import org.opensearch.replication.rest.StopIndexReplicationHandler
import org.opensearch.replication.rest.UpdateAutoFollowPatternsHandler
import org.opensearch.replication.task.IndexCloseListener
import org.opensearch.replication.task.autofollow.AutoFollowExecutor
import org.opensearch.replication.task.autofollow.AutoFollowParams
import org.opensearch.replication.task.index.IndexReplicationExecutor
import org.opensearch.replication.task.index.IndexReplicationParams
import org.opensearch.replication.task.index.IndexReplicationState
import org.opensearch.replication.task.shard.ShardReplicationExecutor
import org.opensearch.replication.task.shard.ShardReplicationParams
import org.opensearch.replication.task.shard.ShardReplicationState
import org.opensearch.replication.util.Injectables
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionResponse
import org.opensearch.client.Client
import org.opensearch.cluster.NamedDiff
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.metadata.RepositoryMetadata
import org.opensearch.cluster.node.DiscoveryNodes
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.CheckedFunction
import org.opensearch.common.ParseField
import org.opensearch.common.component.LifecycleComponent
import org.opensearch.common.io.stream.NamedWriteableRegistry
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.IndexScopedSettings
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.common.settings.SettingsFilter
import org.opensearch.common.settings.SettingsModule
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.env.Environment
import org.opensearch.env.NodeEnvironment
import org.opensearch.index.IndexModule
import org.opensearch.index.IndexSettings
import org.opensearch.index.engine.EngineFactory
import org.opensearch.indices.recovery.RecoverySettings
import org.opensearch.persistent.PersistentTaskParams
import org.opensearch.persistent.PersistentTaskState
import org.opensearch.persistent.PersistentTasksExecutor
import org.opensearch.plugins.ActionPlugin
import org.opensearch.plugins.ActionPlugin.ActionHandler
import org.opensearch.plugins.EnginePlugin
import org.opensearch.plugins.PersistentTaskPlugin
import org.opensearch.plugins.Plugin
import org.opensearch.plugins.RepositoryPlugin
import org.opensearch.repositories.RepositoriesService
import org.opensearch.repositories.Repository
import org.opensearch.rest.RestController
import org.opensearch.rest.RestHandler
import org.opensearch.script.ScriptService
import org.opensearch.threadpool.ExecutorBuilder
import org.opensearch.threadpool.ScalingExecutorBuilder
import org.opensearch.threadpool.ThreadPool
import org.opensearch.watcher.ResourceWatcherService
import java.util.Optional
import java.util.function.Supplier
import org.opensearch.replication.action.index.block.UpdateIndexBlockAction
import org.opensearch.replication.action.index.block.TransportUpddateIndexBlockAction

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
            ActionHandler(ReleaseLeaderResourcesAction.INSTANCE, TransportReleaseLeaderResourcesAction::class.java)
        )
    }

    override fun getRestHandlers(settings: Settings?, restController: RestController,
                                 clusterSettings: ClusterSettings?, indexScopedSettings: IndexScopedSettings,
                                 settingsFilter: SettingsFilter?,
                                 indexNameExpressionResolver: IndexNameExpressionResolver,
                                 nodesInCluster: Supplier<DiscoveryNodes>): List<RestHandler> {
        return listOf(ReplicateIndexHandler(),
            UpdateAutoFollowPatternsHandler(),
            StopIndexReplicationHandler())
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
