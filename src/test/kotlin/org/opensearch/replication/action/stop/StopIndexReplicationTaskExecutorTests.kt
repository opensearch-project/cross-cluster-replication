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

import org.opensearch.Version
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.block.ClusterBlocks
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.common.settings.Settings
import org.opensearch.commons.replication.action.StopIndexReplicationRequest
import org.opensearch.replication.ReplicationPlugin.Companion.REPLICATED_INDEX_SETTING
import org.opensearch.replication.metadata.INDEX_REPLICATION_BLOCK
import org.opensearch.test.OpenSearchTestCase

class StopIndexReplicationTaskExecutorTests : OpenSearchTestCase() {

    private fun indexMetadataWithReplicatedSetting(indexName: String, leaderIndex: String = "leader-$indexName"): IndexMetadata {
        return IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_INDEX_UUID, randomAlphaOfLength(12))
                    .put(REPLICATED_INDEX_SETTING.key, leaderIndex)
            )
            .build()
    }

    private fun clusterStateWithReplicatedIndexAndBlock(indexName: String): ClusterState {
        return ClusterState.builder(ClusterName("test"))
            .metadata(Metadata.builder().put(indexMetadataWithReplicatedSetting(indexName), false))
            .blocks(ClusterBlocks.builder().addIndexBlock(indexName, INDEX_REPLICATION_BLOCK))
            .build()
    }

    fun `test single stop clears block and setting`() {
        val state = clusterStateWithReplicatedIndexAndBlock("follower-1")
        val task = StopIndexReplicationRequest("follower-1")

        val result = StopIndexReplicationTaskExecutor.execute(state, listOf(task))
        val newState = result.resultingState!!

        assertFalse(newState.blocks().hasIndexBlock("follower-1", INDEX_REPLICATION_BLOCK))
        assertNull(newState.metadata.index("follower-1")!!.settings[REPLICATED_INDEX_SETTING.key])
        // settingsVersion should have advanced by 1.
        assertEquals(
            state.metadata.index("follower-1")!!.settingsVersion + 1,
            newState.metadata.index("follower-1")!!.settingsVersion
        )
        assertTrue(result.executionResults[task]!!.isSuccess)
    }

    fun `test stop is idempotent when block is already gone`() {
        // Setting still present, block already removed.
        val state = ClusterState.builder(ClusterName("test"))
            .metadata(Metadata.builder().put(indexMetadataWithReplicatedSetting("follower-1"), false))
            .build()
        val task = StopIndexReplicationRequest("follower-1")

        val result = StopIndexReplicationTaskExecutor.execute(state, listOf(task))
        val newState = result.resultingState!!

        // Setting is now cleared.
        assertNull(newState.metadata.index("follower-1")!!.settings[REPLICATED_INDEX_SETTING.key])
        assertTrue(result.executionResults[task]!!.isSuccess)
    }

    fun `test stop no-op when both block and setting already absent`() {
        // Index exists but no block and no setting.
        val plainIndex = IndexMetadata.builder("follower-1")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_INDEX_UUID, randomAlphaOfLength(12))
            )
            .build()
        val state = ClusterState.builder(ClusterName("test"))
            .metadata(Metadata.builder().put(plainIndex, false))
            .build()

        val task = StopIndexReplicationRequest("follower-1")

        val result = StopIndexReplicationTaskExecutor.execute(state, listOf(task))
        val newState = result.resultingState!!

        // Nothing to change — task still succeeds.
        assertTrue(result.executionResults[task]!!.isSuccess)
        assertEquals(
            state.metadata.index("follower-1")!!.settingsVersion,
            newState.metadata.index("follower-1")!!.settingsVersion
        )
    }

    fun `test batch of N stops on distinct indices produces one state with all clears`() {
        val n = 20
        val metadataBuilder = Metadata.builder()
        val blocksBuilder = ClusterBlocks.builder()
        for (i in 0 until n) {
            metadataBuilder.put(indexMetadataWithReplicatedSetting("follower-$i"), false)
            blocksBuilder.addIndexBlock("follower-$i", INDEX_REPLICATION_BLOCK)
        }
        val state = ClusterState.builder(ClusterName("test"))
            .metadata(metadataBuilder)
            .blocks(blocksBuilder)
            .build()
        val tasks = (0 until n).map { StopIndexReplicationRequest("follower-$it") }

        val result = StopIndexReplicationTaskExecutor.execute(state, tasks)
        val newState = result.resultingState!!

        assertEquals(n, result.executionResults.size)
        for (i in 0 until n) {
            assertFalse(
                "follower-$i should have block removed",
                newState.blocks().hasIndexBlock("follower-$i", INDEX_REPLICATION_BLOCK)
            )
            assertNull(
                "follower-$i should have setting cleared",
                newState.metadata.index("follower-$i")!!.settings[REPLICATED_INDEX_SETTING.key]
            )
        }
        result.executionResults.values.forEach { assertTrue(it.isSuccess) }
    }

    fun `test batch tolerates missing index with per-task success`() {
        // Index-1 exists; index-missing does not. The executor must not fail the batch; both
        // tasks succeed because the block-remove and setting-clear are null-guarded.
        val state = clusterStateWithReplicatedIndexAndBlock("follower-1")
        val tasks = listOf(
            StopIndexReplicationRequest("follower-1"),
            StopIndexReplicationRequest("follower-missing")
        )

        val result = StopIndexReplicationTaskExecutor.execute(state, tasks)
        val newState = result.resultingState!!

        assertTrue(result.executionResults[tasks[0]]!!.isSuccess)
        assertTrue(result.executionResults[tasks[1]]!!.isSuccess) // Idempotent no-op for missing index.
        assertFalse(newState.blocks().hasIndexBlock("follower-1", INDEX_REPLICATION_BLOCK))
    }

    fun `test describeTasks truncates for large batches`() {
        val tasks = (0 until 30).map { StopIndexReplicationRequest("idx-$it") }
        val description = StopIndexReplicationTaskExecutor.describeTasks(tasks)
        assertTrue("Description should mention truncation", description.contains("and 10 more"))
    }
}
