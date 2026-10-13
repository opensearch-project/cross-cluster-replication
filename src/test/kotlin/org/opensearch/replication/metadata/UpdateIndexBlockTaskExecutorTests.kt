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

import org.opensearch.Version
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.block.ClusterBlocks
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.common.settings.Settings
import org.opensearch.replication.action.index.block.IndexBlockUpdateType
import org.opensearch.replication.action.index.block.UpdateIndexBlockRequest
import org.opensearch.test.OpenSearchTestCase

class UpdateIndexBlockTaskExecutorTests : OpenSearchTestCase() {

    private fun clusterStateWithIndex(indexName: String): ClusterState {
        val indexMetadata = IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_INDEX_UUID, randomAlphaOfLength(12))
            )
            .build()
        return ClusterState.builder(ClusterName("test"))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .build()
    }

    fun `test single ADD_BLOCK adds the block`() {
        val initialState = clusterStateWithIndex("index-1")
        val task = UpdateIndexBlockRequest("index-1", IndexBlockUpdateType.ADD_BLOCK)

        val result = UpdateIndexBlockTaskExecutor.execute(initialState, listOf(task))
        val newState = result.resultingState!!

        assertTrue(newState.blocks().hasIndexBlock("index-1", INDEX_REPLICATION_BLOCK))
        assertEquals(1, result.executionResults.size)
        assertTrue(result.executionResults[task]!!.isSuccess)
    }

    fun `test single REMOVE_BLOCK removes the block`() {
        val stateWithBlock = ClusterState.builder(clusterStateWithIndex("index-1"))
            .blocks(
                ClusterBlocks.builder()
                    .addIndexBlock("index-1", INDEX_REPLICATION_BLOCK)
            )
            .build()
        val task = UpdateIndexBlockRequest("index-1", IndexBlockUpdateType.REMOVE_BLOCK)

        val result = UpdateIndexBlockTaskExecutor.execute(stateWithBlock, listOf(task))
        val newState = result.resultingState!!

        assertFalse(newState.blocks().hasIndexBlock("index-1", INDEX_REPLICATION_BLOCK))
        assertTrue(result.executionResults[task]!!.isSuccess)
    }

    fun `test REMOVE_BLOCK is idempotent when block already absent`() {
        val initialState = clusterStateWithIndex("index-1")
        val task = UpdateIndexBlockRequest("index-1", IndexBlockUpdateType.REMOVE_BLOCK)

        val result = UpdateIndexBlockTaskExecutor.execute(initialState, listOf(task))
        val newState = result.resultingState!!

        // No block to remove — should complete cleanly.
        assertFalse(newState.blocks().hasIndexBlock("index-1", INDEX_REPLICATION_BLOCK))
        assertTrue(result.executionResults[task]!!.isSuccess)
    }

    fun `test ADD_BLOCK is idempotent when block already present`() {
        val stateWithBlock = ClusterState.builder(clusterStateWithIndex("index-1"))
            .blocks(
                ClusterBlocks.builder()
                    .addIndexBlock("index-1", INDEX_REPLICATION_BLOCK)
            )
            .build()
        val task = UpdateIndexBlockRequest("index-1", IndexBlockUpdateType.ADD_BLOCK)

        val result = UpdateIndexBlockTaskExecutor.execute(stateWithBlock, listOf(task))
        val newState = result.resultingState!!

        assertTrue(newState.blocks().hasIndexBlock("index-1", INDEX_REPLICATION_BLOCK))
        assertTrue(result.executionResults[task]!!.isSuccess)
    }

    fun `test batch of N ADD_BLOCK across distinct indices coalesces into one state`() {
        val n = 25
        var state = ClusterState.builder(ClusterName("test"))
            .metadata(Metadata.builder())
            .build()
        val tasks = mutableListOf<UpdateIndexBlockRequest>()
        for (i in 0 until n) {
            val name = "index-$i"
            val indexMd = IndexMetadata.builder(name)
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetadata.SETTING_INDEX_UUID, randomAlphaOfLength(12))
                )
                .build()
            state = ClusterState.builder(state).metadata(Metadata.builder(state.metadata).put(indexMd, false)).build()
            tasks.add(UpdateIndexBlockRequest(name, IndexBlockUpdateType.ADD_BLOCK))
        }

        val result = UpdateIndexBlockTaskExecutor.execute(state, tasks)
        val newState = result.resultingState!!

        for (i in 0 until n) {
            assertTrue("index-$i should have the block", newState.blocks().hasIndexBlock("index-$i", INDEX_REPLICATION_BLOCK))
        }
        assertEquals(n, result.executionResults.size)
        result.executionResults.values.forEach { assertTrue(it.isSuccess) }
    }

    fun `test mixed batch of ADD and REMOVE applies each direction correctly`() {
        // Set up state where index-1 already has the block.
        var state = clusterStateWithIndex("index-1")
        state = ClusterState.builder(state)
            .blocks(
                ClusterBlocks.builder()
                    .addIndexBlock("index-1", INDEX_REPLICATION_BLOCK)
            )
            .metadata(
                Metadata.builder(state.metadata).put(
                    IndexMetadata.builder("index-2")
                        .settings(
                            Settings.builder()
                                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                                .put(IndexMetadata.SETTING_INDEX_UUID, randomAlphaOfLength(12))
                        )
                        .build(), false
                )
            )
            .build()

        val addTask = UpdateIndexBlockRequest("index-2", IndexBlockUpdateType.ADD_BLOCK)
        val removeTask = UpdateIndexBlockRequest("index-1", IndexBlockUpdateType.REMOVE_BLOCK)

        val result = UpdateIndexBlockTaskExecutor.execute(state, listOf(addTask, removeTask))
        val newState = result.resultingState!!

        assertTrue(newState.blocks().hasIndexBlock("index-2", INDEX_REPLICATION_BLOCK))
        assertFalse(newState.blocks().hasIndexBlock("index-1", INDEX_REPLICATION_BLOCK))
    }

    fun `test describeTasks truncates for large batches`() {
        val tasks = (0 until 30).map { UpdateIndexBlockRequest("idx-$it", IndexBlockUpdateType.ADD_BLOCK) }
        val description = UpdateIndexBlockTaskExecutor.describeTasks(tasks)
        assertTrue("Description should mention truncation", description.contains("and 10 more"))
    }
}
