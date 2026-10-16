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

package org.opensearch.replication.action.stop

import org.apache.logging.log4j.LogManager
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.ClusterStateTaskExecutor
import org.opensearch.cluster.block.ClusterBlocks
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.common.settings.Settings
import org.opensearch.commons.replication.action.StopIndexReplicationRequest
import org.opensearch.replication.ReplicationPlugin.Companion.REPLICATED_INDEX_SETTING
import org.opensearch.replication.metadata.INDEX_REPLICATION_BLOCK

/**
 * Singleton [ClusterStateTaskExecutor] for the stop-replication mutation
 * (remove [INDEX_REPLICATION_BLOCK] and clear [REPLICATED_INDEX_SETTING]).
 *
 * Because the same object is passed as the executor on every submission, OpenSearch's
 * [org.opensearch.cluster.service.TaskBatcher] uses object identity of this singleton as the
 * batching key: concurrent single-index stops on distinct indices coalesce into one
 * cluster-manager update turn (one publish round, one ack cycle, N listener resumes).
 *
 * The mutation is idempotent per index:
 *   - If the block is already absent, the block-remove is a no-op.
 *   - If the setting is already absent, the metadata rewrite is skipped.
 *
 * Per-task failure attribution: each request's mutation is wrapped in try/catch and reported
 * via [ClusterStateTaskExecutor.ClusterTasksResult.Builder.failure]. A single malformed input
 * does not fail the batch.
 */
object StopIndexReplicationTaskExecutor : ClusterStateTaskExecutor<StopIndexReplicationRequest> {

    private val log = LogManager.getLogger(StopIndexReplicationTaskExecutor::class.java)

    override fun execute(
        currentState: ClusterState,
        tasks: List<StopIndexReplicationRequest>
    ): ClusterStateTaskExecutor.ClusterTasksResult<StopIndexReplicationRequest> {

        val blocksBuilder = ClusterBlocks.builder().blocks(currentState.blocks)
        val metadataBuilder = Metadata.builder(currentState.metadata)
        val resultBuilder = ClusterStateTaskExecutor.ClusterTasksResult.builder<StopIndexReplicationRequest>()

        // In-batch overlays: avoid re-doing work for duplicate same-index submissions in one batch,
        // and prevent reading stale state after a prior task in the same batch has already mutated it.
        val blockRemoved = HashSet<String>()
        val settingCleared = HashSet<String>()

        var mutated = false
        for (task in tasks) {
            try {
                val indexName = task.indexName

                // 1. Remove the replication block if present and not already removed in this batch.
                if (!blockRemoved.contains(indexName) &&
                        currentState.blocks.hasIndexBlock(indexName, INDEX_REPLICATION_BLOCK)) {
                    blocksBuilder.removeIndexBlock(indexName, INDEX_REPLICATION_BLOCK)
                    blockRemoved.add(indexName)
                    mutated = true
                }

                // 2. Clear the leader-index setting if present and not already cleared in this batch.
                //    Bump settingsVersion so shards observe the change.
                if (!settingCleared.contains(indexName)) {
                    val currentIndexMetadata = currentState.metadata.index(indexName)
                    if (currentIndexMetadata != null &&
                            currentIndexMetadata.settings[REPLICATED_INDEX_SETTING.key] != null) {
                        val newIndexMetadata = IndexMetadata.builder(currentIndexMetadata)
                                .settings(
                                    Settings.builder()
                                        .put(currentIndexMetadata.settings)
                                        .putNull(REPLICATED_INDEX_SETTING.key)
                                )
                                .settingsVersion(1 + currentIndexMetadata.settingsVersion)
                        metadataBuilder.put(newIndexMetadata)
                        settingCleared.add(indexName)
                        mutated = true
                    }
                }

                resultBuilder.success(task)
            } catch (e: Exception) {
                log.warn("Failed to apply stop-replication cluster-state mutation for index [${task.indexName}]", e)
                resultBuilder.failure(task, e)
            }
        }

        val newState = if (mutated) {
            ClusterState.builder(currentState)
                .blocks(blocksBuilder)
                .metadata(metadataBuilder)
                .build()
        } else {
            currentState
        }
        return resultBuilder.build(newState)
    }

    override fun describeTasks(tasks: List<StopIndexReplicationRequest>): String {
        val head = tasks.take(20).joinToString { it.indexName }
        return if (tasks.size > 20) "$head, ... and ${tasks.size - 20} more" else head
    }
}
