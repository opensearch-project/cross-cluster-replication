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

package org.opensearch.replication.task.cleanup

import org.opensearch.replication.ReplicationPlugin.Companion.REPLICATED_INDEX_SETTING
import org.opensearch.replication.metadata.INDEX_REPLICATION_BLOCK
import org.opensearch.cluster.service.ClusterService
import org.opensearch.persistent.PersistentTasksCustomMetadata
import org.apache.logging.log4j.LogManager
import org.opensearch.common.inject.Inject

/**
 * Detects and reports stale replication artifacts that may remain after incomplete
 * stop operations or system failures.
 */
class StaleArtifactDetector @Inject constructor(
    private val clusterService: ClusterService
) {
    
    companion object {
        private val log = LogManager.getLogger(StaleArtifactDetector::class.java)
    }

    fun detectStaleArtifacts(indexName: String): StaleArtifactReport {
        log.debug("Detecting stale artifacts for index: $indexName")
        
        val artifacts = mutableListOf<StaleArtifact>()
        
        // Check for replication blocks
        val hasReplicationBlock = detectReplicationBlock(indexName)
        if (hasReplicationBlock) {
            artifacts.add(StaleArtifact(
                type = StaleArtifactType.REPLICATION_BLOCK,
                indexName = indexName,
                description = "Index has replication block that should be removed"
            ))
        }
        
        // Check for replicated index settings
        val hasReplicatedSetting = detectReplicatedIndexSetting(indexName)
        if (hasReplicatedSetting) {
            artifacts.add(StaleArtifact(
                type = StaleArtifactType.REPLICATED_INDEX_SETTING,
                indexName = indexName,
                description = "Index has replicated index setting that should be removed"
            ))
        }
        
        // Check for stale persistent tasks
        val staleTasks = detectStalePersistentTasks(indexName)
        staleTasks.forEach { task ->
            artifacts.add(StaleArtifact(
                type = StaleArtifactType.PERSISTENT_TASK,
                indexName = indexName,
                taskId = task.id,
                description = "Stale persistent task: ${task.id} (assigned: ${task.isAssigned})"
            ))
        }
        
        val report = StaleArtifactReport(
            indexName = indexName,
            artifacts = artifacts,
            hasStaleArtifacts = artifacts.isNotEmpty()
        )
        
        log.info("Stale artifact detection for $indexName: found ${artifacts.size} artifacts")
        return report
    }

    fun detectAllStaleArtifacts(): Map<String, StaleArtifactReport> {
        log.debug("Detecting stale artifacts across all indices")
        
        val reports = mutableMapOf<String, StaleArtifactReport>()
        val clusterState = clusterService.state()
        
        // Get all indices that might have replication artifacts
        val indicesWithBlocks = clusterState.blocks.indices().keys
        val indicesWithSettings = clusterState.metadata.indices.keys.filter { indexName ->
            clusterState.metadata.index(indexName)?.settings?.get(REPLICATED_INDEX_SETTING.key) != null
        }
        val indicesWithTasks = getIndicesWithReplicationTasks()
        
        // Combine all indices that might have stale artifacts
        val allIndices = (indicesWithBlocks + indicesWithSettings + indicesWithTasks).toSet()
        
        allIndices.forEach { indexName ->
            val report = detectStaleArtifacts(indexName)
            if (report.hasStaleArtifacts) {
                reports[indexName] = report
            }
        }
        
        log.info("Cluster-wide stale artifact detection: found artifacts in ${reports.size} indices")
        return reports
    }

    private fun detectReplicationBlock(indexName: String): Boolean {
        return clusterService.state().blocks.hasIndexBlock(indexName, INDEX_REPLICATION_BLOCK)
    }

    private fun detectReplicatedIndexSetting(indexName: String): Boolean {
        return clusterService.state().metadata.index(indexName)
            ?.settings?.get(REPLICATED_INDEX_SETTING.key) != null
    }

    private fun detectStalePersistentTasks(indexName: String): List<PersistentTasksCustomMetadata.PersistentTask<*>> {
        val allTasks: PersistentTasksCustomMetadata? =
            clusterService.state()?.metadata()?.custom(PersistentTasksCustomMetadata.TYPE)
        
        return allTasks?.tasks()?.filter { task ->
            isReplicationTaskForIndex(task, indexName)
        } ?: emptyList()
    }

    private fun getIndicesWithReplicationTasks(): Set<String> {
        val allTasks: PersistentTasksCustomMetadata? =
            clusterService.state()?.metadata()?.custom(PersistentTasksCustomMetadata.TYPE)
        
        return allTasks?.tasks()?.mapNotNull { task ->
            extractIndexNameFromReplicationTask(task)
        }?.toSet() ?: emptySet()
    }

    fun isReplicationTaskForIndex(
        task: PersistentTasksCustomMetadata.PersistentTask<*>,
        indexName: String
    ): Boolean {
        if (!task.id.startsWith("replication:")) {
            return false
        }
        
        // Index replication task format: replication:index:indexName
        if (task.id == "replication:index:$indexName") {
            return true
        }
        
        // Shard replication task format: replication:[indexName][shardId]
        // Example: replication:[index1][0]
        // We need to extract the index name between the first '[' and ']'
        val parts = task.id.split(":")
        if (parts.size >= 2) {
            val shardPart = parts[1]
            // Extract index name from [indexName][shardId] format
            if (shardPart.startsWith("[")) {
                val firstCloseBracket = shardPart.indexOf(']')
                if (firstCloseBracket > 1) {
                    val extractedIndexName = shardPart.substring(1, firstCloseBracket)
                    return extractedIndexName == indexName
                }
            }
        }
        
        return false
    }

    private fun extractIndexNameFromReplicationTask(
        task: PersistentTasksCustomMetadata.PersistentTask<*>
    ): String? {
        // Task ID formats:
        // 1. Index task: replication:index:indexName
        // 2. Shard task: replication:[indexName][shardId]
        
        // Regex pattern explanation:
        // ^replication:           - Must start with "replication:"
        // (?:                     - Non-capturing group for alternatives
        //   index:(.+)            - Match "index:" followed by index name (group 1)
        //   |                     - OR
        //   \[([^\]]+)\]          - Match "[indexName]" - capture text between brackets (group 2)
        // )
        val pattern = Regex("^replication:(?:index:(.+)|\\[([^\\]]+)\\])")
        val matchResult = pattern.find(task.id)
        
        return matchResult?.let {
            // Group 1 is for index task format, Group 2 is for shard task format
            it.groupValues[1].ifEmpty { it.groupValues[2] }
        }
    }
}

data class StaleArtifactReport(
    val indexName: String,
    val artifacts: List<StaleArtifact>,
    val hasStaleArtifacts: Boolean
) {
    fun getArtifactsByType(type: StaleArtifactType): List<StaleArtifact> {
        return artifacts.filter { it.type == type }
    }
}

data class StaleArtifact(
    val type: StaleArtifactType,
    val indexName: String,
    val taskId: String? = null,
    val description: String
)

enum class StaleArtifactType {
    REPLICATION_BLOCK,
    REPLICATED_INDEX_SETTING,
    PERSISTENT_TASK
}