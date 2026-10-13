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

package org.opensearch.replication.metadata

import org.apache.logging.log4j.LogManager
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.ClusterStateTaskExecutor
import org.opensearch.cluster.block.ClusterBlocks
import org.opensearch.replication.action.index.block.IndexBlockUpdateType
import org.opensearch.replication.action.index.block.UpdateIndexBlockRequest

/**
 * Singleton [ClusterStateTaskExecutor] for INDEX_REPLICATION_BLOCK add/remove operations.
 *
 * Because the same object is passed as the executor argument on every submission, OpenSearch's
 * [org.opensearch.cluster.service.TaskBatcher] uses object identity of this singleton as the
 * batching key: concurrent block operations coalesce into a single cluster-manager update turn.
 *
 * Each task input (a [UpdateIndexBlockRequest]) carries its own [IndexBlockUpdateType], so a
 * single batch may contain a mix of ADD and REMOVE operations targeting different indices.
 * The executor applies them in submission order (preserved by TaskBatcher's LinkedHashSet).
 *
 * Failure attribution: each per-task mutation is wrapped in a try/catch and reported via
 * [ClusterStateTaskExecutor.ClusterTasksResult.Builder.failure]; a single malformed input does
 * not fail the batch.
 */
object UpdateIndexBlockTaskExecutor : ClusterStateTaskExecutor<UpdateIndexBlockRequest> {

    private val log = LogManager.getLogger(UpdateIndexBlockTaskExecutor::class.java)

    override fun execute(
        currentState: ClusterState,
        tasks: List<UpdateIndexBlockRequest>
    ): ClusterStateTaskExecutor.ClusterTasksResult<UpdateIndexBlockRequest> {

        val blocksBuilder = ClusterBlocks.builder().blocks(currentState.blocks)
        val resultBuilder = ClusterStateTaskExecutor.ClusterTasksResult.builder<UpdateIndexBlockRequest>()

        // Overlay of in-batch block presence so we avoid rebuilding ClusterBlocks per task (O(N^2)).
        val blocked = HashMap<String, Boolean>()
        fun isBlocked(index: String): Boolean =
            blocked.getOrPut(index) { currentState.blocks.hasIndexBlock(index, INDEX_REPLICATION_BLOCK) }

        var mutated = false
        for (task in tasks) {
            try {
                when (task.updateType) {
                    IndexBlockUpdateType.ADD_BLOCK -> {
                        if (!isBlocked(task.indexName)) {
                            blocksBuilder.addIndexBlock(task.indexName, INDEX_REPLICATION_BLOCK)
                            blocked[task.indexName] = true
                            mutated = true
                        }
                    }
                    IndexBlockUpdateType.REMOVE_BLOCK -> {
                        if (isBlocked(task.indexName)) {
                            blocksBuilder.removeIndexBlock(task.indexName, INDEX_REPLICATION_BLOCK)
                            blocked[task.indexName] = false
                            mutated = true
                        }
                    }
                }
                resultBuilder.success(task)
            } catch (e: Exception) {
                log.warn("Failed to ${task.updateType} on index [${task.indexName}] in batched executor", e)
                resultBuilder.failure(task, e)
            }
        }

        val newState = if (mutated) {
            ClusterState.builder(currentState).blocks(blocksBuilder).build()
        } else {
            currentState
        }
        return resultBuilder.build(newState)
    }

    override fun describeTasks(tasks: List<UpdateIndexBlockRequest>): String {
        // Truncate for large batches to keep log lines bounded.
        val head = tasks.take(20).joinToString { "${it.updateType}:${it.indexName}" }
        return if (tasks.size > 20) "$head, ... and ${tasks.size - 20} more" else head
    }
}
