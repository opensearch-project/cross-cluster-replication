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

package org.opensearch.replication.task.shard

import org.apache.logging.log4j.LogManager
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.ClusterStateUpdateTask
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.common.Priority
import org.opensearch.persistent.PersistentTasksCustomMetadata

/**
 * One-shot cluster state update task that removes legacy [LEGACY_SHARD_TASK_NAME]
 * entries from [PersistentTasksCustomMetadata] in a single batched mutation.
 *
 * Run on the cluster manager during plugin initialization. Idempotent: returns the input state
 * unchanged if no shard task entries are present.
 */
class CleanupShardTasksUpdateTask : ClusterStateUpdateTask(Priority.NORMAL) {

    companion object {
        private val log = LogManager.getLogger(CleanupShardTasksUpdateTask::class.java)
        const val SOURCE = "ccr-cleanup-shard-tasks"

        /**
         * Task name used by the now-removed [ShardReplicationExecutor] in older versions. Hard-coded here so
         * we can identify and remove legacy entries from cluster state without depending on the deleted class.
         */
        const val LEGACY_SHARD_TASK_NAME = "cluster:indices/shards/replication"
    }

    override fun execute(currentState: ClusterState): ClusterState {
        val current = currentState.metadata().custom<PersistentTasksCustomMetadata>(PersistentTasksCustomMetadata.TYPE)
            ?: return currentState

        val shardTaskCount = current.tasks().count { it.taskName == LEGACY_SHARD_TASK_NAME }
        if (shardTaskCount == 0) {
            log.debug("No legacy ShardReplicationTask entries to clean up")
            return currentState
        }

        log.info("Removing $shardTaskCount legacy ShardReplicationTask entries from cluster state")

        // Build a new PersistentTasksCustomMetadata excluding any entries with taskName matching the legacy
        // LEGACY_SHARD_TASK_NAME. Use the builder so we don't depend on exact constructor signatures.
        val builder = PersistentTasksCustomMetadata.builder(current)
        current.tasks()
            .filter { it.taskName == LEGACY_SHARD_TASK_NAME }
            .forEach { builder.removeTask(it.id) }

        val updated = builder.build()
        return ClusterState.builder(currentState)
            .metadata(Metadata.builder(currentState.metadata()).putCustom(PersistentTasksCustomMetadata.TYPE, updated))
            .build()
    }

    override fun onFailure(source: String, e: Exception) {
        log.warn("CleanupShardTasksUpdateTask failed (source=$source): ${e.message}")
    }
}
