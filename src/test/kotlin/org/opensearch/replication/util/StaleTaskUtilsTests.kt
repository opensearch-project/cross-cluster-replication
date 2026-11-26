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

package org.opensearch.replication.util

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.opensearch.Version
import org.opensearch.ResourceNotFoundException
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionType
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksAction
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.node.DiscoveryNodeRole
import org.opensearch.cluster.node.DiscoveryNodes
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.core.index.Index
import org.opensearch.core.index.shard.ShardId
import org.opensearch.persistent.PersistentTaskParams
import org.opensearch.persistent.PersistentTaskResponse
import org.opensearch.persistent.PersistentTasksCustomMetadata
import org.opensearch.persistent.RemovePersistentTaskAction
import org.opensearch.replication.task.index.IndexReplicationExecutor
import org.opensearch.replication.task.index.IndexReplicationParams
import org.opensearch.replication.task.shard.ShardReplicationExecutor
import org.opensearch.replication.task.shard.ShardReplicationParams
import org.opensearch.test.ClusterServiceUtils
import org.opensearch.test.ClusterServiceUtils.setState
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.client.NoOpNodeClient
import org.opensearch.threadpool.TestThreadPool
import java.util.Collections

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class StaleTaskUtilsTests : OpenSearchTestCase() {

    private val followerIndex = "test-follower-index"
    private val otherIndex = "other-index"
    private val threadPool = TestThreadPool("StaleTaskUtilsTests")
    private val clusterService = ClusterServiceUtils.createClusterService(threadPool)

    override fun tearDown() {
        super.tearDown()
        clusterService.close()
        threadPool.shutdown()
    }

    // isReplicationTaskForIndex

    fun testIsReplicationTaskForIndex_matchesIndexTask() {
        val task = buildPersistentTask("replication:index:$followerIndex")
        assertThat(StaleTaskUtils.isReplicationTaskForIndex(task, followerIndex)).isTrue()
    }

    fun testIsReplicationTaskForIndex_matchesShardTask() {
        val task = buildPersistentTask("replication:[$followerIndex][0]")
        assertThat(StaleTaskUtils.isReplicationTaskForIndex(task, followerIndex)).isTrue()
    }

    fun testIsReplicationTaskForIndex_matchesShardTaskMultipleShards() {
        val task0 = buildPersistentTask("replication:[$followerIndex][0]")
        val task1 = buildPersistentTask("replication:[$followerIndex][1]")
        val task5 = buildPersistentTask("replication:[$followerIndex][5]")
        assertThat(StaleTaskUtils.isReplicationTaskForIndex(task0, followerIndex)).isTrue()
        assertThat(StaleTaskUtils.isReplicationTaskForIndex(task1, followerIndex)).isTrue()
        assertThat(StaleTaskUtils.isReplicationTaskForIndex(task5, followerIndex)).isTrue()
    }

    fun testIsReplicationTaskForIndex_doesNotMatchDifferentIndex() {
        val indexTask = buildPersistentTask("replication:index:$otherIndex")
        val shardTask = buildPersistentTask("replication:[$otherIndex][0]")
        assertThat(StaleTaskUtils.isReplicationTaskForIndex(indexTask, followerIndex)).isFalse()
        assertThat(StaleTaskUtils.isReplicationTaskForIndex(shardTask, followerIndex)).isFalse()
    }

    fun testIsReplicationTaskForIndex_doesNotMatchNonReplicationTask() {
        val task = buildPersistentTask("some-other-task:$followerIndex")
        assertThat(StaleTaskUtils.isReplicationTaskForIndex(task, followerIndex)).isFalse()
    }

    fun testIsReplicationTaskForIndex_doesNotMatchPartialIndexName() {
        val task = buildPersistentTask("replication:index:test-follower")
        assertThat(StaleTaskUtils.isReplicationTaskForIndex(task, followerIndex)).isFalse()
    }

    fun testIsReplicationTaskForIndex_doesNotMatchSupersetIndexName() {
        val task = buildPersistentTask("replication:index:${followerIndex}-extra")
        assertThat(StaleTaskUtils.isReplicationTaskForIndex(task, followerIndex)).isFalse()
    }

    // removeStaleTasksForIndex

    fun testRemoveStaleTasksForIndex_returnsZeroWhenNoTasks() = runBlocking {
        val metadata = Metadata.builder().build()
        val newState = ClusterState.builder(clusterService.state()).metadata(metadata).build()
        setState(clusterService, newState)

        val client = StaleTaskTestClient("noTasks")
        val removed = StaleTaskUtils.removeStaleTasksForIndex(clusterService, client, followerIndex)
        assertThat(removed).isEqualTo(0)
    }

    fun testRemoveStaleTasksForIndex_removesUnassignedTasks() = runBlocking {
        val tasks = PersistentTasksCustomMetadata.builder()
        tasks.addTask<PersistentTaskParams>(
            "replication:index:$followerIndex",
            IndexReplicationExecutor.TASK_NAME,
            IndexReplicationParams("remote", Index(followerIndex, "_na_"), followerIndex),
            PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT
        )
        setClusterStateWithTasks(tasks.build())

        val client = StaleTaskTestClient("unassigned")
        val removed = StaleTaskUtils.removeStaleTasksForIndex(clusterService, client, followerIndex)
        assertThat(removed).isEqualTo(1)
        assertThat(client.removedTaskIds).hasSize(1)
    }

    fun testRemoveStaleTasksForIndex_removesTasksOnInvalidNode() = runBlocking {
        val tasks = PersistentTasksCustomMetadata.builder()
        tasks.addTask<PersistentTaskParams>(
            "replication:index:$followerIndex",
            IndexReplicationExecutor.TASK_NAME,
            IndexReplicationParams("remote", Index(followerIndex, "_na_"), followerIndex),
            PersistentTasksCustomMetadata.Assignment("gone_node", "test")
        )
        // Set cluster state with NO data nodes — so "gone_node" is invalid
        setClusterStateWithTasksAndNodes(tasks.build(), emptyList())

        val client = StaleTaskTestClient("invalidNode")
        val removed = StaleTaskUtils.removeStaleTasksForIndex(clusterService, client, followerIndex)
        assertThat(removed).isEqualTo(1)
        assertThat(client.removedTaskIds).hasSize(1)
    }

    fun testRemoveStaleTasksForIndex_removesTaskNotRunningOnValidNode() = runBlocking {
        val tasks = PersistentTasksCustomMetadata.builder()
        tasks.addTask<PersistentTaskParams>(
            "replication:index:$followerIndex",
            IndexReplicationExecutor.TASK_NAME,
            IndexReplicationParams("remote", Index(followerIndex, "_na_"), followerIndex),
            PersistentTasksCustomMetadata.Assignment("valid_node", "test")
        )
        // Node exists but ListTasks returns empty — task not running
        setClusterStateWithTasksAndNodes(tasks.build(), listOf("valid_node"))

        val client = StaleTaskTestClient("notRunning", runningDescriptions = emptySet())
        val removed = StaleTaskUtils.removeStaleTasksForIndex(clusterService, client, followerIndex)
        assertThat(removed).isEqualTo(1)
        assertThat(client.removedTaskIds).hasSize(1)
    }

    fun testRemoveStaleTasksForIndex_handlesResourceNotFoundExceptionIdempotently() = runBlocking {
        val tasks = PersistentTasksCustomMetadata.builder()
        tasks.addTask<PersistentTaskParams>(
            "replication:index:$followerIndex",
            IndexReplicationExecutor.TASK_NAME,
            IndexReplicationParams("remote", Index(followerIndex, "_na_"), followerIndex),
            PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT
        )
        setClusterStateWithTasks(tasks.build())

        // Client throws ResourceNotFoundException on remove — task already gone
        val client = StaleTaskTestClient("alreadyRemoved", removeThrows = ResourceNotFoundException("already gone"))
        val removed = StaleTaskUtils.removeStaleTasksForIndex(clusterService, client, followerIndex)
        assertThat(removed).isEqualTo(1) // idempotent success
    }

    fun testRemoveStaleTasksForIndex_handlesWrappedResourceNotFoundException() = runBlocking {
        val tasks = PersistentTasksCustomMetadata.builder()
        tasks.addTask<PersistentTaskParams>(
            "replication:index:$followerIndex",
            IndexReplicationExecutor.TASK_NAME,
            IndexReplicationParams("remote", Index(followerIndex, "_na_"), followerIndex),
            PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT
        )
        setClusterStateWithTasks(tasks.build())

        // Wrapped ResourceNotFoundException
        val wrappedException = RuntimeException("wrapper", ResourceNotFoundException("already gone"))
        val client = StaleTaskTestClient("wrappedRNFE", removeThrows = wrappedException)
        val removed = StaleTaskUtils.removeStaleTasksForIndex(clusterService, client, followerIndex)
        assertThat(removed).isEqualTo(1) // idempotent success via hasCause
    }

    fun testRemoveStaleTasksForIndex_returnsFalseOnOtherException() = runBlocking {
        val tasks = PersistentTasksCustomMetadata.builder()
        tasks.addTask<PersistentTaskParams>(
            "replication:index:$followerIndex",
            IndexReplicationExecutor.TASK_NAME,
            IndexReplicationParams("remote", Index(followerIndex, "_na_"), followerIndex),
            PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT
        )
        setClusterStateWithTasks(tasks.build())

        val client = StaleTaskTestClient("otherError", removeThrows = RuntimeException("something broke"))
        val removed = StaleTaskUtils.removeStaleTasksForIndex(clusterService, client, followerIndex)
        assertThat(removed).isEqualTo(0) // failure
    }

    fun testRemoveStaleTasksForIndex_mixedUnassignedAndAssigned() = runBlocking {
        val tasks = PersistentTasksCustomMetadata.builder()
        // Unassigned index task
        tasks.addTask<PersistentTaskParams>(
            "replication:index:$followerIndex",
            IndexReplicationExecutor.TASK_NAME,
            IndexReplicationParams("remote", Index(followerIndex, "_na_"), followerIndex),
            PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT
        )
        // Assigned shard task on valid node, not running
        val shardId = ShardId(Index(followerIndex, "_na_"), 0)
        tasks.addTask<PersistentTaskParams>(
            "replication:[$followerIndex][0]",
            ShardReplicationExecutor.TASK_NAME,
            ShardReplicationParams("remote", shardId, shardId),
            PersistentTasksCustomMetadata.Assignment("valid_node", "test")
        )
        // Task for a different index — should not be touched
        tasks.addTask<PersistentTaskParams>(
            "replication:index:$otherIndex",
            IndexReplicationExecutor.TASK_NAME,
            IndexReplicationParams("remote", Index(otherIndex, "_na_"), otherIndex),
            PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT
        )
        setClusterStateWithTasksAndNodes(tasks.build(), listOf("valid_node"))

        val client = StaleTaskTestClient("mixed", runningDescriptions = emptySet())
        val removed = StaleTaskUtils.removeStaleTasksForIndex(clusterService, client, followerIndex)
        assertThat(removed).isEqualTo(2)
        assertThat(client.removedTaskIds).hasSize(2)
    }

    // removeAllTasksForIndex

    fun testRemoveAllTasksForIndex_returnsZeroWhenNoTasks() = runBlocking {
        val metadata = Metadata.builder().build()
        val newState = ClusterState.builder(clusterService.state()).metadata(metadata).build()
        setState(clusterService, newState)

        val client = StaleTaskTestClient("allNoTasks")
        val removed = StaleTaskUtils.removeAllTasksForIndex(clusterService, client, followerIndex)
        assertThat(removed).isEqualTo(0)
    }

    fun testRemoveAllTasksForIndex_removesUnassignedTask() = runBlocking {
        val tasks = PersistentTasksCustomMetadata.builder()
        tasks.addTask<PersistentTaskParams>(
            "replication:index:$followerIndex",
            IndexReplicationExecutor.TASK_NAME,
            IndexReplicationParams("remote", Index(followerIndex, "_na_"), followerIndex),
            PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT
        )
        setClusterStateWithTasks(tasks.build())

        val client = StaleTaskTestClient("allUnassigned")
        val removed = StaleTaskUtils.removeAllTasksForIndex(clusterService, client, followerIndex)
        assertThat(removed).isEqualTo(1)
        assertThat(client.removedTaskIds).hasSize(1)
    }

    fun testRemoveAllTasksForIndex_removesAssignedRunningTask() = runBlocking {
        val tasks = PersistentTasksCustomMetadata.builder()
        tasks.addTask<PersistentTaskParams>(
            "replication:index:$followerIndex",
            IndexReplicationExecutor.TASK_NAME,
            IndexReplicationParams("remote", Index(followerIndex, "_na_"), followerIndex),
            PersistentTasksCustomMetadata.Assignment("valid_node", "test")
        )
        setClusterStateWithTasksAndNodes(tasks.build(), listOf("valid_node"))

        val client = StaleTaskTestClient("allRunning")
        val removed = StaleTaskUtils.removeAllTasksForIndex(clusterService, client, followerIndex)
        assertThat(removed).isEqualTo(1)
        assertThat(client.removedTaskIds).hasSize(1)
    }

    fun testRemoveAllTasksForIndex_removesMixedTasks() = runBlocking {
        val tasks = PersistentTasksCustomMetadata.builder()
        // Unassigned index task
        tasks.addTask<PersistentTaskParams>(
            "replication:index:$followerIndex",
            IndexReplicationExecutor.TASK_NAME,
            IndexReplicationParams("remote", Index(followerIndex, "_na_"), followerIndex),
            PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT
        )
        // Assigned shard task on valid node
        val shardId = ShardId(Index(followerIndex, "_na_"), 0)
        tasks.addTask<PersistentTaskParams>(
            "replication:[$followerIndex][0]",
            ShardReplicationExecutor.TASK_NAME,
            ShardReplicationParams("remote", shardId, shardId),
            PersistentTasksCustomMetadata.Assignment("valid_node", "test")
        )
        // Task for a different index — should not be touched
        tasks.addTask<PersistentTaskParams>(
            "replication:index:$otherIndex",
            IndexReplicationExecutor.TASK_NAME,
            IndexReplicationParams("remote", Index(otherIndex, "_na_"), otherIndex),
            PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT
        )
        setClusterStateWithTasksAndNodes(tasks.build(), listOf("valid_node"))

        val client = StaleTaskTestClient("allMixed")
        val removed = StaleTaskUtils.removeAllTasksForIndex(clusterService, client, followerIndex)
        assertThat(removed).isEqualTo(2)
        assertThat(client.removedTaskIds).hasSize(2)
    }

    fun testRemoveAllTasksForIndex_handlesRemovalFailure() = runBlocking {
        val tasks = PersistentTasksCustomMetadata.builder()
        tasks.addTask<PersistentTaskParams>(
            "replication:index:$followerIndex",
            IndexReplicationExecutor.TASK_NAME,
            IndexReplicationParams("remote", Index(followerIndex, "_na_"), followerIndex),
            PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT
        )
        setClusterStateWithTasks(tasks.build())

        val client = StaleTaskTestClient("allFail", removeThrows = RuntimeException("something broke"))
        val removed = StaleTaskUtils.removeAllTasksForIndex(clusterService, client, followerIndex)
        assertThat(removed).isEqualTo(0)
    }

    fun testRemoveAllTasksForIndex_handlesResourceNotFoundIdempotently() = runBlocking {
        val tasks = PersistentTasksCustomMetadata.builder()
        tasks.addTask<PersistentTaskParams>(
            "replication:index:$followerIndex",
            IndexReplicationExecutor.TASK_NAME,
            IndexReplicationParams("remote", Index(followerIndex, "_na_"), followerIndex),
            PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT
        )
        setClusterStateWithTasks(tasks.build())

        val client = StaleTaskTestClient("allAlreadyGone", removeThrows = ResourceNotFoundException("already gone"))
        val removed = StaleTaskUtils.removeAllTasksForIndex(clusterService, client, followerIndex)
        assertThat(removed).isEqualTo(1)
    }

    //Helpers

    private fun buildPersistentTask(
        taskId: String,
        assignment: PersistentTasksCustomMetadata.Assignment = PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT
    ): PersistentTasksCustomMetadata.PersistentTask<*> {
        val tasks = PersistentTasksCustomMetadata.builder()
        tasks.addTask<PersistentTaskParams>(
            taskId,
            IndexReplicationExecutor.TASK_NAME,
            IndexReplicationParams("remote", Index(followerIndex, "_na_"), followerIndex),
            assignment
        )
        return tasks.build().getTask(taskId)!!
    }

    private fun setClusterStateWithTasks(persistentTasks: PersistentTasksCustomMetadata) {
        val metadata = Metadata.builder()
            .putCustom(PersistentTasksCustomMetadata.TYPE, persistentTasks)
            .build()
        val newState = ClusterState.builder(clusterService.state()).metadata(metadata).build()
        setState(clusterService, newState)
    }

    private fun setClusterStateWithTasksAndNodes(
        persistentTasks: PersistentTasksCustomMetadata,
        dataNodeIds: List<String>
    ) {
        val metadata = Metadata.builder()
            .putCustom(PersistentTasksCustomMetadata.TYPE, persistentTasks)
            .build()
        val nodesBuilder = DiscoveryNodes.builder()
        for (nodeId in dataNodeIds) {
            nodesBuilder.add(DiscoveryNode(nodeId, buildNewFakeTransportAddress(), Collections.emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT))
        }
        val newState = ClusterState.builder(clusterService.state())
            .metadata(metadata)
            .nodes(nodesBuilder.build())
            .build()
        setState(clusterService, newState)
    }

    /**
     * Test client that tracks removed task IDs and can simulate ListTasks responses.
     * Overrides threadPool() to provide the test thread pool needed by suspendExecute.
     */
    inner class StaleTaskTestClient(
        testName: String,
        private val runningDescriptions: Set<String> = emptySet(),
        private val removeThrows: Exception? = null
    ) : NoOpNodeClient(testName) {

        val removedTaskIds = mutableListOf<String>()

        override fun <Request : ActionRequest, Response : ActionResponse> doExecute(
            action: ActionType<Response>?,
            request: Request?,
            listener: ActionListener<Response>
        ) {
            when (action) {
                RemovePersistentTaskAction.INSTANCE -> {
                    if (removeThrows != null) {
                        listener.onFailure(removeThrows)
                    } else {
                        removedTaskIds.add("removed")
                        val response = PersistentTaskResponse(null as PersistentTasksCustomMetadata.PersistentTask<*>?)
                        listener.onResponse(response as Response)
                    }
                }
                ListTasksAction.INSTANCE -> {
                    val response = ListTasksResponse(emptyList(), emptyList(), emptyList())
                    listener.onResponse(response as Response)
                }
                else -> {
                    listener.onFailure(UnsupportedOperationException("Unexpected action: ${action?.name()}"))
                }
            }
        }
    }
}
