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

package org.opensearch.replication.task.index

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.spy
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.mockito.Mockito
import org.opensearch.Version
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.ClusterStateObserver
import org.opensearch.cluster.RestoreInProgress
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.node.DiscoveryNodes
import org.opensearch.cluster.routing.RoutingTable
import org.opensearch.common.settings.Settings
import org.opensearch.common.settings.SettingsModule
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.index.Index
import org.opensearch.index.shard.ShardId
import org.opensearch.persistent.PersistentTaskParams
import org.opensearch.persistent.PersistentTasksCustomMetadata
import org.opensearch.persistent.PersistentTasksService
import org.opensearch.replication.ReplicationPlugin
import org.opensearch.replication.ReplicationSettings
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.metadata.ReplicationOverallState
import org.opensearch.replication.metadata.store.ReplicationContext
import org.opensearch.replication.metadata.store.ReplicationMetadata
import org.opensearch.replication.metadata.store.ReplicationMetadataStore
import org.opensearch.replication.metadata.store.ReplicationMetadataStore.Companion.REPLICATION_CONFIG_SYSTEM_INDEX
import org.opensearch.replication.metadata.store.ReplicationStoreMetadataType
import org.opensearch.replication.repository.REMOTE_REPOSITORY_PREFIX
import org.opensearch.replication.task.shard.ShardReplicationExecutor
import org.opensearch.replication.task.shard.ShardReplicationParams
import org.opensearch.snapshots.Snapshot
import org.opensearch.snapshots.SnapshotId
import org.opensearch.tasks.TaskId.EMPTY_TASK_ID
import org.opensearch.tasks.TaskManager
import org.opensearch.test.ClusterServiceUtils
import org.opensearch.test.ClusterServiceUtils.setState
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.TestThreadPool
import java.util.*
import java.util.concurrent.TimeUnit

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class IndexReplicationTaskTests : OpenSearchTestCase()  {

    companion object {
        var currentTaskState :IndexReplicationState = InitialState
        var stateChanges :Int = 0
        var restoreNotNull = false

        var followerIndex = "follower-index"
        var connectionName = "leader-cluster"
        var remoteCluster = "remote-cluster"
    }

    var threadPool = TestThreadPool("ReplicationPluginTest")
    var clusterService  = ClusterServiceUtils.createClusterService(threadPool)

    fun testExecute() = runBlocking {
        val replicationTask: IndexReplicationTask = spy(createIndexReplicationTask())
        var taskManager = Mockito.mock(TaskManager::class.java)
        replicationTask.setPersistent(taskManager)
        var rc = ReplicationContext(followerIndex)
        var rm = ReplicationMetadata(connectionName, ReplicationStoreMetadataType.INDEX.name, ReplicationOverallState.RUNNING.name, "reason", rc, rc, Settings.EMPTY)
        replicationTask.setReplicationMetadata(rm)

        //Update ClusterState to say restore started
        val state: ClusterState = clusterService.state()

        var newClusterState: ClusterState

        // Updating cluster state
        var builder: ClusterState.Builder
        val indices: MutableList<String> = ArrayList()
        indices.add(followerIndex)
        val snapshot = Snapshot("$REMOTE_REPOSITORY_PREFIX$connectionName", SnapshotId("randomAlphaOfLength", "randomAlphaOfLength"))
        val restoreEntry = RestoreInProgress.Entry("restoreUUID", snapshot, RestoreInProgress.State.INIT, Collections.unmodifiableList(ArrayList<String>(indices)),
                null)

        // Update metadata store index as well
        var metaBuilder = Metadata.builder()
        metaBuilder.put(IndexMetadata.builder(REPLICATION_CONFIG_SYSTEM_INDEX).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
        var metadata = metaBuilder.build()
        var routingTableBuilder = RoutingTable.builder()
        routingTableBuilder.addAsNew(metadata.index(REPLICATION_CONFIG_SYSTEM_INDEX))
        var routingTable = routingTableBuilder.build()

        builder = ClusterState.builder(state).routingTable(routingTable)
        builder.putCustom(RestoreInProgress.TYPE, RestoreInProgress.Builder(
                state.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)).add(restoreEntry).build())

        newClusterState = builder.build()
        setState(clusterService, newClusterState)

        val job = this.launch{
            replicationTask.execute(this, InitialState)
        }

        // Delay to let task execute
        delay(1000)

        // Assert we move to RESTORING .. This is blocking and won't let the test run
        assertBusy({
            assertThat(currentTaskState == RestoreState).isTrue()
        },  1, TimeUnit.SECONDS)


        //Complete the Restore
        metaBuilder = Metadata.builder()
        metaBuilder.put(IndexMetadata.builder(followerIndex).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
        metadata = metaBuilder.build()
        routingTableBuilder = RoutingTable.builder()
        routingTableBuilder.addAsNew(metadata.index(followerIndex))
        routingTable = routingTableBuilder.build()

        builder = ClusterState.builder(state).routingTable(routingTable)
        builder.putCustom(RestoreInProgress.TYPE, RestoreInProgress.Builder(
                state.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)).build())

        newClusterState = builder.build()
        setState(clusterService, newClusterState)

        delay(1000)

        assertBusy {
            assertThat(currentTaskState == MonitoringState).isTrue()
        }

        job.cancel()

    }

    fun testStartNewShardTasks() = runBlocking {
        val replicationTask: IndexReplicationTask = spy(createIndexReplicationTask())
        var taskManager = Mockito.mock(TaskManager::class.java)
        replicationTask.setPersistent(taskManager)
        var rc = ReplicationContext(followerIndex)
        var rm = ReplicationMetadata(connectionName, ReplicationStoreMetadataType.INDEX.name, ReplicationOverallState.RUNNING.name, "reason", rc, rc, Settings.EMPTY)
        replicationTask.setReplicationMetadata(rm)

        // Build cluster state
        val indices: MutableList<String> = ArrayList()
        indices.add(followerIndex)
        var metadata = Metadata.builder()
            .put(IndexMetadata.builder(REPLICATION_CONFIG_SYSTEM_INDEX).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .put(IndexMetadata.builder(followerIndex).settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(0))
            .build()
        var routingTableBuilder = RoutingTable.builder()
            .addAsNew(metadata.index(REPLICATION_CONFIG_SYSTEM_INDEX))
            .addAsNew(metadata.index(followerIndex))
        var newClusterState = ClusterState.builder(clusterService.state()).routingTable(routingTableBuilder.build()).build()
        setState(clusterService, newClusterState)

        // Try starting shard tasks
        val shardTasks = replicationTask.startNewOrMissingShardTasks()
        assert(shardTasks.size == 2)
    }


    fun testStartMissingShardTasks() = runBlocking {
        val replicationTask: IndexReplicationTask = spy(createIndexReplicationTask())
        var taskManager = Mockito.mock(TaskManager::class.java)
        replicationTask.setPersistent(taskManager)
        var rc = ReplicationContext(followerIndex)
        var rm = ReplicationMetadata(connectionName, ReplicationStoreMetadataType.INDEX.name, ReplicationOverallState.RUNNING.name, "reason", rc, rc, Settings.EMPTY)
        replicationTask.setReplicationMetadata(rm)

        // Build cluster state
        val indices: MutableList<String> = ArrayList()
        indices.add(followerIndex)

        val tasks = PersistentTasksCustomMetadata.builder()
        var sId = ShardId(Index(followerIndex, "_na_"), 0)
        tasks.addTask<PersistentTaskParams>( "replication:0", ShardReplicationExecutor.TASK_NAME, ShardReplicationParams("remoteCluster", sId,  sId),
            PersistentTasksCustomMetadata.Assignment("other_node_", "test assignment on other node"))

        var metadata = Metadata.builder()
            .put(IndexMetadata.builder(REPLICATION_CONFIG_SYSTEM_INDEX).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .put(IndexMetadata.builder(followerIndex).settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(0))
            .putCustom(PersistentTasksCustomMetadata.TYPE, tasks.build())
            .build()
        var routingTableBuilder = RoutingTable.builder()
            .addAsNew(metadata.index(REPLICATION_CONFIG_SYSTEM_INDEX))
            .addAsNew(metadata.index(followerIndex))
        var newClusterState = ClusterState.builder(clusterService.state()).routingTable(routingTableBuilder.build()).build()
        setState(clusterService, newClusterState)

        // Try starting shard tasks
        val shardTasks = replicationTask.startNewOrMissingShardTasks()
        assert(shardTasks.size == 2)
    }

    fun testIsTrackingTaskForIndex() = runBlocking {
        val replicationTask: IndexReplicationTask = spy(createIndexReplicationTask())
        var taskManager = Mockito.mock(TaskManager::class.java)
        replicationTask.setPersistent(taskManager)
        var rc = ReplicationContext(followerIndex)
        var rm = ReplicationMetadata(connectionName, ReplicationStoreMetadataType.INDEX.name, ReplicationOverallState.RUNNING.name, "reason", rc, rc, Settings.EMPTY)
        replicationTask.setReplicationMetadata(rm)

        // when index replication task is valid
        var tasks = PersistentTasksCustomMetadata.builder()
        var leaderIndex = Index(followerIndex, "_na_")
        tasks.addTask<PersistentTaskParams>( "replication:0", IndexReplicationExecutor.TASK_NAME, IndexReplicationParams("remoteCluster", leaderIndex,  followerIndex),
                PersistentTasksCustomMetadata.Assignment("same_node", "test assignment on other node"))

        var metadata = Metadata.builder()
                .put(IndexMetadata.builder(REPLICATION_CONFIG_SYSTEM_INDEX).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
                .put(IndexMetadata.builder(followerIndex).settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(0))
                .putCustom(PersistentTasksCustomMetadata.TYPE, tasks.build())
                .build()
        var routingTableBuilder = RoutingTable.builder()
                .addAsNew(metadata.index(REPLICATION_CONFIG_SYSTEM_INDEX))
                .addAsNew(metadata.index(followerIndex))
        var discoveryNodesBuilder = DiscoveryNodes.Builder()
                .localNodeId("same_node")
        var newClusterState = ClusterState.builder(clusterService.state())
                .metadata(metadata)
                .routingTable(routingTableBuilder.build())
                .nodes(discoveryNodesBuilder.build()).build()
        setState(clusterService, newClusterState)
        assert(replicationTask.isTrackingTaskForIndex() == true)

        // when index replication task is not valid
        tasks = PersistentTasksCustomMetadata.builder()
        leaderIndex = Index(followerIndex, "_na_")
        tasks.addTask<PersistentTaskParams>( "replication:0", IndexReplicationExecutor.TASK_NAME, IndexReplicationParams("remoteCluster", leaderIndex,  followerIndex),
                PersistentTasksCustomMetadata.Assignment("other_node", "test assignment on other node"))

        metadata = Metadata.builder()
                .put(IndexMetadata.builder(REPLICATION_CONFIG_SYSTEM_INDEX).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
                .put(IndexMetadata.builder(followerIndex).settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(0))
                .putCustom(PersistentTasksCustomMetadata.TYPE, tasks.build())
                .build()
        routingTableBuilder = RoutingTable.builder()
                .addAsNew(metadata.index(REPLICATION_CONFIG_SYSTEM_INDEX))
                .addAsNew(metadata.index(followerIndex))
        discoveryNodesBuilder = DiscoveryNodes.Builder()
                .localNodeId("same_node")
        newClusterState = ClusterState.builder(clusterService.state())
                .metadata(metadata)
                .routingTable(routingTableBuilder.build())
                .nodes(discoveryNodesBuilder.build()).build()
        setState(clusterService, newClusterState)
        assert(replicationTask.isTrackingTaskForIndex() == false)
    }

    private fun createIndexReplicationTask() : IndexReplicationTask {
        var threadPool = TestThreadPool("IndexReplicationTask")
        //Hack Alert : Though it is meant to force rejection , this is to make overallTaskScope not null
        threadPool.startForcingRejections()
        clusterService = ClusterServiceUtils.createClusterService(threadPool)
        val settingsModule = Mockito.mock(SettingsModule::class.java)
        val spyClient = Mockito.spy<NoOpClient>(NoOpClient("testName"))

        val replicationMetadataManager = ReplicationMetadataManager(clusterService, spyClient,
                ReplicationMetadataStore(spyClient, clusterService, NamedXContentRegistry.EMPTY))
        var persist = PersistentTasksService(clusterService, threadPool, spyClient)
        val state: ClusterState = clusterService.state()
        val tasks = PersistentTasksCustomMetadata.builder()
        var sId = ShardId(Index(followerIndex, "_na_"), 0)
        tasks.addTask<PersistentTaskParams>( "replication:0", ShardReplicationExecutor.TASK_NAME, ShardReplicationParams("remoteCluster", sId,  sId),
                PersistentTasksCustomMetadata.Assignment("other_node_", "test assignment on other node"))

        val metadata = Metadata.builder(state.metadata())
        metadata.putCustom(PersistentTasksCustomMetadata.TYPE, tasks.build())
        val newClusterState: ClusterState = ClusterState.builder(state).metadata(metadata).build()

        setState(clusterService, newClusterState)

        doAnswer{  invocation -> spyClient }.`when`(spyClient).getRemoteClusterClient(any())
        assert(spyClient.getRemoteClusterClient(remoteCluster) == spyClient)

        val replicationSettings = Mockito.mock(ReplicationSettings::class.java)
        replicationSettings.metadataSyncInterval = TimeValue(100, TimeUnit.MILLISECONDS)
        val cso = ClusterStateObserver(clusterService, logger, threadPool.threadContext)
        val indexReplicationTask = IndexReplicationTask(1, "type", "action", "description" , EMPTY_TASK_ID,
                ReplicationPlugin.REPLICATION_EXECUTOR_NAME_FOLLOWER, clusterService , threadPool, spyClient, IndexReplicationParams(connectionName, Index(followerIndex, "0"), followerIndex),
                persist, replicationMetadataManager, replicationSettings, settingsModule,cso)

        return indexReplicationTask
    }
}
