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

import org.apache.logging.log4j.LogManager
import org.opensearch.ResourceNotFoundException
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksAction
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest
import org.opensearch.cluster.service.ClusterService
import org.opensearch.persistent.PersistentTasksCustomMetadata
import org.opensearch.persistent.RemovePersistentTaskAction
import org.opensearch.replication.task.index.IndexReplicationParams
import org.opensearch.transport.client.Client

/**
 * Utility object for stale replication task detection and cleanup.
 * Replication tasks follow these ID patterns:
 * - Index task: "replication:index:{indexName}"
 * - Shard task: "replication:[{indexName}][{shardId}]"
 * A task is considered stale if:
 * It is unassigned (no executor node), OR
 * t is assigned but the executor node is no longer part of the cluster, OR
 * It is assigned but the corresponding task is not running in the task manager
 * All operations are idempotent — calling them multiple times with the same
 * inputs produces the same result.
 */
object StaleTaskUtils {

    private val log = LogManager.getLogger(StaleTaskUtils::class.java)

    /**
     * Shard task pattern: replication:[indexName][shardId]
     * Captures the index name from the first bracket group and the shard id from the second.
     */
    private val SHARD_TASK_PATTERN = Regex("^replication:\\[([^\\]]+)\\]\\[(\\d+)\\]$")

    //  Finds all replication tasks for the given index from the cluster state.
    private fun findReplicationTasksForIndex(
        clusterService: ClusterService,
        indexName: String
    ): List<PersistentTasksCustomMetadata.PersistentTask<*>> {
        val allTasks = clusterService.state().metadata
            .custom<PersistentTasksCustomMetadata>(PersistentTasksCustomMetadata.TYPE)
            ?: return emptyList()

        return allTasks.tasks().filter { task ->
            isReplicationTaskForIndex(task, indexName)
        }
    }

    // Determines whether a persistent task is a replication task for the given index.
    fun isReplicationTaskForIndex(
        task: PersistentTasksCustomMetadata.PersistentTask<*>,
        indexName: String
    ): Boolean {
        val taskId = task.id
        if (!taskId.startsWith("replication:")) return false

        // Index task format: replication:index:{indexName}
        if (taskId == "replication:index:$indexName") return true

        // Shard task format: replication:[{indexName}][{shardId}]
        val match = SHARD_TASK_PATTERN.find(taskId)
        return match?.groupValues?.get(1) == indexName
    }

    /**
     * Removes stale replication tasks for the given index from the cluster state
     * A task is removed only if it is truly stale:
     * Unassigned tasks are always removed
     * Assigned tasks are removed only if the assigned node is no longer in the cluster
     * or the task is not actually running in the task manager on that node
     * @return the number of tasks successfully removed
     */
    suspend fun removeStaleTasksForIndex(
        clusterService: ClusterService,
        client: Client,
        indexName: String
    ): Int {
        log.info("Starting stale task cleanup for index $indexName")

        val replicationTasks = findReplicationTasksForIndex(clusterService, indexName)
        if (replicationTasks.isEmpty()) {
            log.info("No replication tasks found for index $indexName")
            return 0
        }

        log.info("Found ${replicationTasks.size} replication task(s) for index $indexName: ${replicationTasks.map { it.id }}")

        val clusterState = clusterService.state()
        val validNodeIds = clusterState.nodes().dataNodes.keys

        // Group tasks by executor node (null for unassigned)
        val tasksByNode = replicationTasks.groupBy {
            if (it.isAssigned) it.assignment.executorNode else null
        }

        var removed = 0

        // Remove all unassigned tasks immediately
        tasksByNode[null]?.forEach { task ->
            if (removeTask(client, task, indexName)) removed++
        }

        // Process assigned tasks per node
        for ((nodeId, tasks) in tasksByNode) {
            if (nodeId == null) continue

            // If the assigned node is no longer in the cluster, remove all its tasks
            if (!validNodeIds.contains(nodeId)) {
                log.info("Node $nodeId is no longer in the cluster, removing its tasks")
                tasks.forEach { task ->
                    if (removeTask(client, task, indexName)) removed++
                }
                continue
            }

            // Node is valid — check task manager to see if tasks are actually running
            val runningDescriptions = getRunningTaskDescriptions(client, nodeId)
            for (task in tasks) {
                if (!isTaskRunningOnNode(task, runningDescriptions)) {
                    log.info("Task ${task.id} is assigned to node $nodeId but not running in task manager")
                    if (removeTask(client, task, indexName)) removed++
                }
            }
        }

        log.info("Successfully cleaned up $removed stale task(s) for index $indexName")
        return removed
    }

    /**
     * Removes ALL replication tasks for the given index from the cluster state,
     * regardless of whether they are stale or actively running.
     * Used by the Start and Resume APIs to ensure a clean slate before creating new tasks.
     * @return the number of tasks successfully removed
     */
    suspend fun removeAllTasksForIndex(
        clusterService: ClusterService,
        client: Client,
        indexName: String
    ): Int {
        log.info("Removing all replication tasks for index $indexName")

        val replicationTasks = findReplicationTasksForIndex(clusterService, indexName)
        if (replicationTasks.isEmpty()) {
            log.info("No replication tasks found for index $indexName")
            return 0
        }

        log.info("Found ${replicationTasks.size} replication task(s) for index $indexName: ${replicationTasks.map { it.id }}")

        var removed = 0
        replicationTasks.forEach { task ->
            if (removeTask(client, task, indexName)) removed++
        }

        log.info("Removed $removed task(s) for index $indexName")
        return removed
    }

    // Removes a single persistent task from the cluster state.
    private suspend fun removeTask(
        client: Client,
        task: PersistentTasksCustomMetadata.PersistentTask<*>,
        indexName: String
    ): Boolean {
        return try {
            log.info("Removing task: ${task.id} from cluster state")
            val removeRequest = RemovePersistentTaskAction.Request(task.id)
            client.suspendExecute(RemovePersistentTaskAction.INSTANCE, removeRequest)
            log.info("Removed stale task ${task.id} for index $indexName")
            true
        } catch (e: ResourceNotFoundException) {
            log.debug("Stale task ${task.id} already removed for index $indexName")
            true
        } catch (e: Exception) {
            if (hasCause(e, ResourceNotFoundException::class.java)) {
                log.debug("Stale task ${task.id} already removed for index $indexName")
                true
            } else {
                log.error("Failed to remove task ${task.id}: ${e.message}")
                false
            }
        }
    }

    //  Queries the task manager on a specific node to get descriptions of running replication tasks.
    private suspend fun getRunningTaskDescriptions(client: Client, nodeId: String): Set<String> {
        return try {
            val listTasksRequest = ListTasksRequest()
                .setActions("cluster:indices/admin/replication*", "cluster:indices/shards/replication*")
                .setNodes(nodeId)
                .setDetailed(true)

            val response = client.suspendExecute(ListTasksAction.INSTANCE, listTasksRequest)
            response.tasks.mapNotNull { it.description }.toSet()
        } catch (e: Exception) {
            log.error("Failed to fetch running tasks from node $nodeId: ${e.message}")
            emptySet()
        }
    }

    /**
     * Checks if a persistent task is actually running on its assigned node by matching
     * the task's params against the running task descriptions from the task manager.
     *
     * Per-shard ShardReplicationParams was removed when shard replication moved to in-memory
     * NodeReplicationController; legacy shard task entries can still appear in cluster state
     * during the migration window before [CleanupShardTasksUpdateTask] runs. We detect those
     * by task-id pattern and build the same regex the deleted ShardReplicationParams branch
     * would have produced.
     */
    private fun isTaskRunningOnNode(
        persistentTask: PersistentTasksCustomMetadata.PersistentTask<*>,
        runningDescriptions: Set<String>
    ): Boolean {
        val params = persistentTask.params
        val pattern = when {
            params is IndexReplicationParams ->
                Regex("->\\s+${Regex.escape(params.followerIndexName)}\\s*$")
            else -> {
                // Fallback for legacy shard task entries: derive from task id.
                val match = SHARD_TASK_PATTERN.find(persistentTask.id) ?: return true
                val followerIndex = match.groupValues[1]
                val shardId = match.groupValues[2]
                Regex("->\\s+\\[${Regex.escape(followerIndex)}\\]\\[${shardId}\\]\\s*$")
            }
        }

        return runningDescriptions.any { pattern.containsMatchIn(it) }
    }

    //  Checks if the given exception or any of its causes is an instance of the specified type.
    private fun hasCause(e: Throwable, type: Class<out Throwable>): Boolean {
        var current: Throwable? = e
        while (current != null) {
            if (type.isInstance(current)) return true
            current = current.cause
        }
        return false
    }
}
