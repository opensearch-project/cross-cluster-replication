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
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.assertj.core.api.Assertions.assertThat
import org.mockito.Mockito
import org.opensearch.Version
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.ClusterStateObserver
import org.opensearch.cluster.RestoreInProgress
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.node.DiscoveryNodeRole
import org.opensearch.cluster.node.DiscoveryNodes
import org.opensearch.cluster.routing.RoutingTable
import org.opensearch.cluster.routing.IndexRoutingTable
import org.opensearch.cluster.routing.ShardRoutingState
import org.opensearch.cluster.routing.TestShardRouting
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Settings
import org.opensearch.common.settings.SettingsModule
import org.opensearch.index.IndexSettings
import org.opensearch.common.unit.TimeValue
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.xcontent.DeprecationHandler
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.index.Index
import org.opensearch.core.index.shard.ShardId
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
import org.opensearch.core.tasks.TaskId.EMPTY_TASK_ID
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
        var restoreFails = false
        var primaryRecovering = false
        var leaderUnreachable = false

        // Recorded by NoOpClient so cleanup()'s effects can be asserted.
        val deletedIndices :MutableList<String> = mutableListOf()
        var removedRetentionLeases :Int = 0
        val cleanupActions :MutableList<String> = mutableListOf()

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
        assertThat(shardTasks.size == 2).isTrue
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
        assertThat(shardTasks.size == 2).isTrue
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
        assertThat(replicationTask.isTrackingTaskForIndex()).isTrue

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
        assertThat(replicationTask.isTrackingTaskForIndex()).isFalse
    }

    /** A bootstrap that fails out of RESTORING must leave cleanup() able to remove the partial restore. */
    fun testBootstrapFailureRemovesPartialRestore() = runBlocking {
        val replicationTask: IndexReplicationTask = spy(createIndexReplicationTask())
        replicationTask.setPersistent(Mockito.mock(TaskManager::class.java))
        val rc = ReplicationContext(followerIndex)
        replicationTask.setReplicationMetadata(ReplicationMetadata(connectionName, ReplicationStoreMetadataType.INDEX.name,
                ReplicationOverallState.RUNNING.name, "reason", rc, rc, Settings.EMPTY))

        val snapshot = Snapshot("$REMOTE_REPOSITORY_PREFIX$connectionName", SnapshotId("snapshotName", "snapshotUUID"))
        val localNodeId = clusterService.state().nodes.localNodeId

        // The follower index is deliberately absent from the routing table, otherwise the task treats this
        // as a resume and skips the restore entirely.
        setState(clusterService, ClusterState.builder(clusterService.state())
                .metadata(metadataWithAssignedIndexTask(localNodeId))
                .routingTable(RoutingTable.builder()
                        .addAsNew(storeIndexMetadata())
                        .build())
                .putCustom(RestoreInProgress.TYPE, RestoreInProgress.Builder()
                        .add(RestoreInProgress.Entry("restoreUUID", snapshot, RestoreInProgress.State.INIT,
                                listOf(followerIndex), null))
                        .build())
                .build())

        val job = this.launch { replicationTask.execute(this, InitialState) }
        awaitTaskState(RestoreState)

        // The restore created the follower index and then failed, which is what leaves the half restored
        // index behind.
        val failedShard = ShardId(Index(followerIndex, "_na_"), 0)
        val followerMetadata = Metadata.builder(metadataWithAssignedIndexTask(localNodeId))
                .put(followerIndexMetadata())
                .build()
        setState(clusterService, ClusterState.builder(clusterService.state())
                .metadata(followerMetadata)
                .routingTable(RoutingTable.builder()
                        .addAsNew(storeIndexMetadata())
                        .addAsNew(followerMetadata.index(followerIndex))
                        .build())
                .putCustom(RestoreInProgress.TYPE, RestoreInProgress.Builder()
                        .add(RestoreInProgress.Entry("restoreUUID", snapshot, RestoreInProgress.State.FAILURE,
                                listOf(followerIndex),
                                mapOf(failedShard to RestoreInProgress.ShardRestoreStatus(localNodeId,
                                        RestoreInProgress.State.FAILURE, "no such index [leader-index]"))))
                        .build())
                .build())

        withTimeout(30000) { job.join() }

        assertThat(currentTaskState).isInstanceOf(FailedState::class.java)
        assertThat((currentTaskState as FailedState).duringRestore).isTrue()

        replicationTask.runCleanup()

        assertThat(deletedIndices).containsExactly(followerIndex)
        assertThat(removedRetentionLeases)
                .`as`("leader side retention leases taken during bootstrap must be released")
                .isEqualTo(1)
        assertThat(cleanupActions)
                .`as`("leases go first: once the index is gone a retried _start can take the same lease ID")
                .containsExactly("lease", "delete")
    }

    /** An unreachable leader must not stop the delete that clears the RED index. */
    fun testCancelRestoreDeletesWhenLeaderUnreachable() = runBlocking {
        val replicationTask = newTask()
        leaderUnreachable = true
        replicationTask.cancelRestoreLeaseTimeoutMs = 100
        runFailedTaskThenCleanup(FailedState(emptyMap(), "restore failed", duringRestore = true), task = replicationTask)
        assertThat(deletedIndices).containsExactly(followerIndex)
        assertThat(removedRetentionLeases).isEqualTo(0)
    }

    /** A new task for the index on this node (a retried _start) is not ours; its restore must survive. */
    fun testCleanupKeepsRestoreOfNewTaskOnSameNode() = runBlocking {
        runFailedTaskThenCleanup(FailedState(emptyMap(), "restore failed", duringRestore = true),
                task = newTask(allocationId = 2))
        assertThat(deletedIndices).isEmpty()
    }

    /** Setup can fail before execute() sets the task state; cleanup() must still run safely. */
    fun testCleanupBeforeExecuteDoesNotThrow() = runBlocking {
        newTask().runCleanup()
        assertThat(deletedIndices).isEmpty()
    }

    /** A vanished restore entry with unassigned primaries is a partial restore to remove. */
    fun testVanishedRestoreWithUnassignedPrimariesIsDuringRestore() = runBlocking {
        assertThat(failureAfterRestoreEntryVanishes(primaryActive = false).duringRestore).isTrue()
    }

    /** Validation also fails while a restored primary relocates; that index must not be removed. */
    fun testVanishedRestoreWithActivePrimariesIsNotDuringRestore() = runBlocking {
        assertThat(failureAfterRestoreEntryVanishes(primaryActive = true).duringRestore).isFalse()
    }

    private suspend fun failureAfterRestoreEntryVanishes(primaryActive: Boolean): FailedState = coroutineScope {
        val replicationTask = newTask()
        val snapshot = Snapshot("$REMOTE_REPOSITORY_PREFIX$connectionName", SnapshotId("snapshotName", "snapshotUUID"))
        val localNodeId = clusterService.state().nodes.localNodeId
        setState(clusterService, ClusterState.builder(clusterService.state())
                .metadata(metadataWithAssignedIndexTask(localNodeId))
                .routingTable(RoutingTable.builder().addAsNew(storeIndexMetadata()).build())
                .putCustom(RestoreInProgress.TYPE, RestoreInProgress.Builder()
                        .add(RestoreInProgress.Entry("restoreUUID", snapshot, RestoreInProgress.State.INIT,
                                listOf(followerIndex), null))
                        .build())
                .build())

        val job = this.launch { replicationTask.execute(this, InitialState) }
        awaitTaskState(RestoreState)

        // The entry is gone and a primary recovery is in flight, so validation fails either way.
        primaryRecovering = true
        val followerMetadata = Metadata.builder(metadataWithAssignedIndexTask(localNodeId))
                .put(followerIndexMetadata())
                .build()
        val routing = RoutingTable.builder().addAsNew(storeIndexMetadata())
        if (primaryActive) {
            val shardId = ShardId(followerMetadata.index(followerIndex).index, 0)
            routing.add(IndexRoutingTable.builder(shardId.index)
                    .addShard(TestShardRouting.newShardRouting(shardId, localNodeId, true, ShardRoutingState.STARTED)))
        } else {
            routing.addAsNew(followerMetadata.index(followerIndex))
        }
        setState(clusterService, ClusterState.builder(clusterService.state())
                .metadata(followerMetadata)
                .routingTable(routing.build())
                .putCustom(RestoreInProgress.TYPE, RestoreInProgress.Builder().build())
                .build())

        withTimeout(30000) { job.join() }
        currentTaskState as FailedState
    }

    /** A task reassigned or restarted into a persisted restore failure must still remove the partial restore. */
    fun testCleanupRemovesPartialRestoreWhenResumedFromRestoreFailure() = runBlocking {
        runFailedTaskThenCleanup(FailedState(emptyMap(), "restore failed", duringRestore = true))
        assertThat(deletedIndices).containsExactly(followerIndex)
    }

    /** markAsFailed() removes the task asynchronously, so cleanup() can run after it is gone. */
    fun testCleanupRemovesPartialRestoreWhenTaskAlreadyRemoved() = runBlocking {
        runFailedTaskThenCleanup(FailedState(emptyMap(), "restore failed", duringRestore = true), taskPresent = false)
        assertThat(deletedIndices).containsExactly(followerIndex)
    }

    /** A failure after the restore completed must never delete the follower index. */
    fun testCleanupKeepsFollowerIndexOnFailureAfterRestore() = runBlocking {
        runFailedTaskThenCleanup(FailedState(emptyMap(), "shard task failed"))
        assertThat(deletedIndices).isEmpty()
    }

    /** An index that took the follower's name before the restore ran is not ours to delete. */
    fun testCleanupKeepsIndexNotCreatedByReplication() = runBlocking {
        runFailedTaskThenCleanup(FailedState(emptyMap(), "index already exists", duringRestore = true),
                replicated = false)
        assertThat(deletedIndices).isEmpty()
        assertThat(removedRetentionLeases).isEqualTo(0)
    }

    /** _stop strips the replicated setting before cancelling the task, so a cancel in RESTORING must not need it. */
    fun testStopDuringRestoreRemovesPartialRestore() = runBlocking {
        val replicationTask = newTask()
        val snapshot = Snapshot("$REMOTE_REPOSITORY_PREFIX$connectionName", SnapshotId("snapshotName", "snapshotUUID"))
        val localNodeId = clusterService.state().nodes.localNodeId
        setState(clusterService, ClusterState.builder(clusterService.state())
                .metadata(metadataWithAssignedIndexTask(localNodeId))
                .routingTable(RoutingTable.builder().addAsNew(storeIndexMetadata()).build())
                .putCustom(RestoreInProgress.TYPE, RestoreInProgress.Builder()
                        .add(RestoreInProgress.Entry("restoreUUID", snapshot, RestoreInProgress.State.INIT,
                                listOf(followerIndex), null))
                        .build())
                .build())

        val job = this.launch { replicationTask.execute(this, InitialState) }
        awaitTaskState(RestoreState)

        // The restore is still running and _stop has already stripped the setting.
        val followerMetadata = Metadata.builder(metadataWithAssignedIndexTask(localNodeId))
                .put(followerIndexMetadata(replicated = false))
                .build()
        setState(clusterService, ClusterState.builder(clusterService.state())
                .metadata(followerMetadata)
                .routingTable(RoutingTable.builder()
                        .addAsNew(storeIndexMetadata())
                        .addAsNew(followerMetadata.index(followerIndex))
                        .build())
                .build())
        job.cancel()

        replicationTask.runCleanup()

        assertThat(deletedIndices).containsExactly(followerIndex)
    }
    /** The restore call can fail after creating the follower index; that failure must also be cleaned up. */
    fun testRestoreCallFailureRemovesPartialRestore() = runBlocking {
        val replicationTask = newTask()
        restoreFails = true
        // In metadata but not the routing table, so the task starts a restore instead of resuming.
        setState(clusterService, ClusterState.builder(clusterService.state())
                .metadata(Metadata.builder(metadataWithAssignedIndexTask(clusterService.state().nodes.localNodeId))
                        .put(followerIndexMetadata()))
                .routingTable(RoutingTable.builder().addAsNew(storeIndexMetadata()).build())
                .build())

        withTimeout(30000) { replicationTask.execute(this, InitialState) }

        assertThat((currentTaskState as FailedState).duringRestore).isTrue()
        replicationTask.runCleanup()
        assertThat(deletedIndices).containsExactly(followerIndex)
    }

    fun testFailedStateDuringRestoreSurvivesWireAndXContent() {
        val state = FailedState(emptyMap(), "restore failed", duringRestore = true)

        assertThat(wireRoundTrip(state, Version.CURRENT)).isEqualTo(state)
        assertThat((wireRoundTrip(state, Version.V_3_9_0) as FailedState).duringRestore)
                .`as`("older nodes neither send nor read the field")
                .isFalse()

        val json = BytesReference.bytes(state.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
        assertThat((parseState(json) as FailedState).duringRestore)
                .`as`("persistent task state is written to disk as XContent, so a full restart reads it back this way")
                .isTrue()
    }

    /** State persisted by a node without the field must read back as not during restore. */
    fun testFailedStateFromOlderXContentIsNotDuringRestore() {
        val json = BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                .field("error_message", "failed").field("state", "FAILED").endObject())
        assertThat((parseState(json) as FailedState).duringRestore).isFalse()
    }

    private fun parseState(json: BytesReference): IndexReplicationState {
        val parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json.streamInput())
        return IndexReplicationState.fromXContent(parser)
    }

    private fun wireRoundTrip(state: IndexReplicationState, version: Version): IndexReplicationState {
        val out = BytesStreamOutput()
        out.version = version
        state.writeTo(out)
        val inp = out.bytes().streamInput()
        inp.version = version
        return IndexReplicationState.reader(inp)
    }

    private suspend fun runFailedTaskThenCleanup(initialState: FailedState, taskPresent: Boolean = true,
                                                 replicated: Boolean = true, task: IndexReplicationTask? = null) {
        val replicationTask = task ?: newTask()
        // Core keeps an empty tasks custom once the last task is removed, rather than dropping it.
        val base = if (taskPresent) metadataWithAssignedIndexTask(clusterService.state().nodes.localNodeId)
                   else Metadata.builder().put(storeIndexMetadata(), false)
                           .putCustom(PersistentTasksCustomMetadata.TYPE, PersistentTasksCustomMetadata.builder().build())
                           .build()
        val followerMetadata = Metadata.builder(base).put(followerIndexMetadata(replicated)).build()
        setState(clusterService, ClusterState.builder(clusterService.state())
                .metadata(followerMetadata)
                .routingTable(RoutingTable.builder()
                        .addAsNew(storeIndexMetadata())
                        .addAsNew(followerMetadata.index(followerIndex))
                        .build())
                .build())

        withTimeout(30000) { replicationTask.execute(this, initialState) }
        replicationTask.runCleanup()
    }

    private suspend fun newTask(allocationId: Long = 1): IndexReplicationTask {
        val replicationTask: IndexReplicationTask = spy(createIndexReplicationTask())
        replicationTask.setPersistent(Mockito.mock(TaskManager::class.java), allocationId)
        val rc = ReplicationContext(followerIndex)
        replicationTask.setReplicationMetadata(ReplicationMetadata(connectionName, ReplicationStoreMetadataType.INDEX.name,
                ReplicationOverallState.RUNNING.name, "reason", rc, rc, Settings.EMPTY))
        return replicationTask
    }

    /** Follower index as the restore creates it: [replicated] controls the setting the restore adds. */
    private fun followerIndexMetadata(replicated: Boolean = true): IndexMetadata.Builder {
        val settings = settings(Version.CURRENT)
        if (replicated) settings.put(ReplicationPlugin.REPLICATED_INDEX_SETTING.key, "leader-index")
        return IndexMetadata.builder(followerIndex).settings(settings).numberOfShards(1).numberOfReplicas(0)
    }

    /** Polls without blocking the thread: the task runs on the same runBlocking dispatcher. */
    private suspend fun awaitTaskState(expected: IndexReplicationState) {
        withTimeout(10000) {
            while (currentTaskState != expected) delay(50)
        }
    }

    /**
     * The mirror of the test above: once the restore has finished, cleanup() must leave the follower index
     * alone. Deleting it here would destroy a live replicated index.
     */
    fun testCleanupKeepsFollowerIndexAfterRestoreCompletes() = runBlocking {
        val replicationTask: IndexReplicationTask = spy(createIndexReplicationTask())
        replicationTask.setPersistent(Mockito.mock(TaskManager::class.java))
        val rc = ReplicationContext(followerIndex)
        replicationTask.setReplicationMetadata(ReplicationMetadata(connectionName, ReplicationStoreMetadataType.INDEX.name,
                ReplicationOverallState.RUNNING.name, "reason", rc, rc, Settings.EMPTY))

        val snapshot = Snapshot("$REMOTE_REPOSITORY_PREFIX$connectionName", SnapshotId("snapshotName", "snapshotUUID"))
        val localNodeId = clusterService.state().nodes.localNodeId

        setState(clusterService, ClusterState.builder(clusterService.state())
                .metadata(metadataWithAssignedIndexTask(localNodeId))
                .routingTable(RoutingTable.builder()
                        .addAsNew(storeIndexMetadata())
                        .build())
                .putCustom(RestoreInProgress.TYPE, RestoreInProgress.Builder()
                        .add(RestoreInProgress.Entry("restoreUUID", snapshot, RestoreInProgress.State.INIT,
                                listOf(followerIndex), null))
                        .build())
                .build())

        val job = this.launch { replicationTask.execute(this, InitialState) }
        awaitTaskState(RestoreState)

        // Restore finished: the entry is gone from cluster state and the follower index is in place.
        val followerMetadata = Metadata.builder(metadataWithAssignedIndexTask(localNodeId))
                .put(followerIndexMetadata())
                .build()
        setState(clusterService, ClusterState.builder(clusterService.state())
                .metadata(followerMetadata)
                .routingTable(RoutingTable.builder()
                        .addAsNew(storeIndexMetadata())
                        .addAsNew(followerMetadata.index(followerIndex))
                        .build())
                .putCustom(RestoreInProgress.TYPE, RestoreInProgress.Builder().build())
                .build())

        awaitTaskState(MonitoringState)
        job.cancel()

        replicationTask.runCleanup()

        assertThat(deletedIndices)
                .`as`("a fully restored follower index must never be removed by cleanup()")
                .isEmpty()
    }

    /** Metadata carrying an index replication task for [followerIndex] assigned to this node. */
    private fun metadataWithAssignedIndexTask(localNodeId: String): Metadata {
        val tasks = PersistentTasksCustomMetadata.builder()
        tasks.addTask<PersistentTaskParams>("replication:index:$followerIndex", IndexReplicationExecutor.TASK_NAME,
                IndexReplicationParams(connectionName, Index(followerIndex, "0"), followerIndex),
                PersistentTasksCustomMetadata.Assignment(localNodeId, "assigned to this node"))
        return Metadata.builder()
                .put(IndexMetadata.builder(storeIndexMetadata()))
                .putCustom(PersistentTasksCustomMetadata.TYPE, tasks.build())
                .build()
    }

    private fun storeIndexMetadata(): IndexMetadata =
            IndexMetadata.builder(REPLICATION_CONFIG_SYSTEM_INDEX).settings(settings(Version.CURRENT))
                    .numberOfShards(1).numberOfReplicas(0).build()

    private fun createIndexReplicationTask() : IndexReplicationTask {
        deletedIndices.clear()
        cleanupActions.clear()
        // awaitTaskState() polls this, so a value left by the previous test would satisfy it early.
        currentTaskState = InitialState
        restoreFails = false
        primaryRecovering = false
        leaderUnreachable = false
        removedRetentionLeases = 0
        var threadPool = TestThreadPool("IndexReplicationTask")
        //Hack Alert : Though it is meant to force rejection , this is to make overallTaskScope not null
        threadPool.startForcingRejections()
        // cleanup() reads REPLICATION_REPLICATE_INDEX_DELETION, which throws unless it is registered.
        val clusterSettings = ClusterSettings(Settings.EMPTY,
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS + ReplicationPlugin.REPLICATION_REPLICATE_INDEX_DELETION)
        val localNode = DiscoveryNode("node", buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT)
        clusterService = ClusterServiceUtils.createClusterService(threadPool, localNode, clusterSettings)
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

    fun testConditionallyOpenIndex() = runBlocking {
        val replicationTask: IndexReplicationTask = spy(createIndexReplicationTask())
        val taskManager = Mockito.mock(TaskManager::class.java)
        replicationTask.setPersistent(taskManager)
        val rc = ReplicationContext(followerIndex)
        val rm = ReplicationMetadata(connectionName, ReplicationStoreMetadataType.INDEX.name, ReplicationOverallState.RUNNING.name, "reason", rc, rc, Settings.EMPTY)
        replicationTask.setReplicationMetadata(rm)

        // Test case 1: Index is closed - should trigger open operation
        var metadata = Metadata.builder()
            .put(IndexMetadata.builder(REPLICATION_CONFIG_SYSTEM_INDEX).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .put(IndexMetadata.builder(followerIndex).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0).state(IndexMetadata.State.CLOSE))
            .build()
        var routingTableBuilder = RoutingTable.builder()
            .addAsNew(metadata.index(REPLICATION_CONFIG_SYSTEM_INDEX))
        var newClusterState = ClusterState.builder(clusterService.state()).metadata(metadata).routingTable(routingTableBuilder.build()).build()
        setState(clusterService, newClusterState)

        // Test with closed index - should return true
        val isClosedResult = replicationTask.isIndexClosed(followerIndex)
        assertThat(isClosedResult).isTrue()

        // Test case 2: Index is open - should return false
        metadata = Metadata.builder()
            .put(IndexMetadata.builder(REPLICATION_CONFIG_SYSTEM_INDEX).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .put(IndexMetadata.builder(followerIndex).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0).state(IndexMetadata.State.OPEN))
            .build()
        routingTableBuilder = RoutingTable.builder()
            .addAsNew(metadata.index(REPLICATION_CONFIG_SYSTEM_INDEX))
        newClusterState = ClusterState.builder(clusterService.state()).metadata(metadata).routingTable(routingTableBuilder.build()).build()
        setState(clusterService, newClusterState)

        val isOpenResult = replicationTask.isIndexClosed(followerIndex)
        assertThat(isOpenResult).isFalse()
        
        // Test case 3: Index metadata not found - should default to true (safe fallback)
        metadata = Metadata.builder()
            .put(IndexMetadata.builder(REPLICATION_CONFIG_SYSTEM_INDEX).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .build()
        routingTableBuilder = RoutingTable.builder()
            .addAsNew(metadata.index(REPLICATION_CONFIG_SYSTEM_INDEX))
        newClusterState = ClusterState.builder(clusterService.state()).metadata(metadata).routingTable(routingTableBuilder.build()).build()
        setState(clusterService, newClusterState)

        val isMissingResult = replicationTask.isIndexClosed("non-existent-index")
        assertThat(isMissingResult).isTrue() // Should default to true for safety
    }

    fun testIsIndexClosedMethod() = runBlocking {
        val replicationTask: IndexReplicationTask = spy(createIndexReplicationTask())
        val taskManager = Mockito.mock(TaskManager::class.java)
        replicationTask.setPersistent(taskManager)
        val rc = ReplicationContext(followerIndex)
        val rm = ReplicationMetadata(connectionName, ReplicationStoreMetadataType.INDEX.name, ReplicationOverallState.RUNNING.name, "reason", rc, rc, Settings.EMPTY)
        replicationTask.setReplicationMetadata(rm)

        // Test case 1: Index is closed
        var metadata = Metadata.builder()
            .put(IndexMetadata.builder(followerIndex).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0).state(IndexMetadata.State.CLOSE))
            .build()
        var newClusterState = ClusterState.builder(clusterService.state()).metadata(metadata).build()
        setState(clusterService, newClusterState)

        val isClosedResult = replicationTask.isIndexClosed(followerIndex)
        assertThat(isClosedResult).isTrue()

        // Test case 2: Index is open
        metadata = Metadata.builder()
            .put(IndexMetadata.builder(followerIndex).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0).state(IndexMetadata.State.OPEN))
            .build()
        newClusterState = ClusterState.builder(clusterService.state()).metadata(metadata).build()
        setState(clusterService, newClusterState)

        val isOpenResult = replicationTask.isIndexClosed(followerIndex)
        assertThat(isOpenResult).isFalse()

        // Test case 3: Index metadata not found - should return true (safe fallback)
        metadata = Metadata.builder().build()
        newClusterState = ClusterState.builder(clusterService.state()).metadata(metadata).build()
        setState(clusterService, newClusterState)

        val isMissingResult = replicationTask.isIndexClosed("non-existent-index")
        assertThat(isMissingResult).isTrue() // Should default to true for safety
    }

    fun testShouldSkipNumberOfReplicasWhenAutoExpandIsActive() {
        // auto_expand_replicas = "0-all" → should skip number_of_replicas
        val followerSettings = Settings.builder()
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all")
            .build()
        assertThat(IndexReplicationTask.shouldSkipSettingSync(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, followerSettings)).isTrue()
    }

    fun testShouldSkipNumberOfReplicasWhenAutoExpandIsRange() {
        // auto_expand_replicas = "0-5" → should skip number_of_replicas
        val followerSettings = Settings.builder()
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-5")
            .build()
        assertThat(IndexReplicationTask.shouldSkipSettingSync(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, followerSettings)).isTrue()
    }

    fun testShouldNotSkipNumberOfReplicasWhenAutoExpandIsFalse() {
        // auto_expand_replicas = "false" → should NOT skip number_of_replicas
        val followerSettings = Settings.builder()
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "false")
            .build()
        assertThat(IndexReplicationTask.shouldSkipSettingSync(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, followerSettings)).isFalse()
    }

    fun testShouldNotSkipNumberOfReplicasWhenAutoExpandIsAbsent() {
        // auto_expand_replicas not set → should NOT skip number_of_replicas
        val followerSettings = Settings.builder().build()
        assertThat(IndexReplicationTask.shouldSkipSettingSync(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, followerSettings)).isFalse()
    }

    fun testShouldNotSkipOtherSettingsWhenAutoExpandIsActive() {
        // auto_expand_replicas is active, but the key is NOT number_of_replicas → should NOT skip
        val followerSettings = Settings.builder()
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all")
            .build()
        assertThat(IndexReplicationTask.shouldSkipSettingSync(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.key, followerSettings)).isFalse()
    }
}