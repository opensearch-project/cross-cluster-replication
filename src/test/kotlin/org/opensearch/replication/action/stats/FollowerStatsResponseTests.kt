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

package org.opensearch.replication.action.stats

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import org.opensearch.Version
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.node.DiscoveryNodeRole
import org.opensearch.core.index.Index
import org.opensearch.core.index.shard.ShardId
import org.opensearch.replication.metadata.ReplicationOverallState
import org.opensearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import org.opensearch.replication.metadata.state.ReplicationStateMetadata
import org.opensearch.replication.task.shard.FollowerShardMetric
import org.opensearch.test.OpenSearchTestCase
import java.util.Collections

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class FollowerStatsResponseTests : OpenSearchTestCase() {

    private val clusterName = ClusterName("test-cluster")

    private fun createNode(nodeId: String): DiscoveryNode {
        return DiscoveryNode(
            nodeId, buildNewFakeTransportAddress(), Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT
        )
    }

    private fun createShardId(indexName: String, shardNum: Int): ShardId {
        return ShardId(Index(indexName, "_na_"), shardNum)
    }

    private fun createMetadata(vararg entries: Pair<String, String>): ReplicationStateMetadata {
        val details = entries.associate { (indexName, state) ->
            indexName to mapOf(REPLICATION_LAST_KNOWN_OVERALL_STATE to state)
        }
        return ReplicationStateMetadata(details)
    }

    private fun createNodeResponse(
        nodeId: String,
        shardMetrics: Map<ShardId, FollowerShardMetric>
    ): FollowerNodeStatsResponse {
        return FollowerNodeStatsResponse(createNode(nodeId), shardMetrics)
    }

    /**
     * Covers all code paths in a single mixed-state scenario:
     * - RUNNING + live shard tasks → syncing
     * - RUNNING + no shard tasks → bootstrapping
     * - PAUSED → pausedIndices
     * - FAILED → failedIndices
     * - STOPPED → ignored (not counted)
     * - Orphaned shard tasks (in-memory but not in metadata) → ignored, no negative counts
     * - Multi-node aggregation
     * - indexStats aggregation for syncing index
     */
    fun `test follower stats computation from single source of truth`() {
        val metadata = createMetadata(
            "index-syncing" to ReplicationOverallState.RUNNING.name,
            "index-bootstrapping" to ReplicationOverallState.RUNNING.name,
            "index-paused" to ReplicationOverallState.PAUSED.name,
            "index-failed" to ReplicationOverallState.FAILED.name,
            "index-stopped" to ReplicationOverallState.STOPPED.name
        )

        // Node 1: has shard tasks for index-syncing and an orphaned index not in metadata
        val metric = FollowerShardMetric()
        metric.opsWritten.set(100)
        metric.opsRead.set(200)

        val node1Metrics = mapOf(
            createShardId("index-syncing", 0) to metric,
            createShardId("orphan-stale-index", 0) to FollowerShardMetric()
        )

        // Node 2: has another shard for index-syncing (multi-node distribution)
        val node2Metrics = mapOf(
            createShardId("index-syncing", 1) to FollowerShardMetric()
        )

        val nodeResponses = listOf(
            createNodeResponse("node-1", node1Metrics),
            createNodeResponse("node-2", node2Metrics)
        )

        val response = FollowerStatsResponse(clusterName, nodeResponses, emptyList(), metadata)

        // RUNNING + live shard tasks = syncing
        assertEquals(1, response.syncingIndices)
        // RUNNING + no shard tasks = bootstrapping
        assertEquals(1, response.bootstrappingIndices)
        // PAUSED counted
        assertEquals(1, response.pausedIndices)
        // FAILED counted
        assertEquals(1, response.failedIndices)
        // indexTaskCount = syncing + bootstrapping
        assertEquals(2, response.indexTaskCount)
        // shardTaskCount = total shard entries across all nodes (including orphan)
        assertEquals(3, response.shardTaskCount)
        // Orphaned shard tasks do NOT produce negative bootstrapping
        assertTrue(response.bootstrappingIndices >= 0)
        // Index stats aggregated for syncing index
        assertTrue(response.indexStats.containsKey("index-syncing"))
        assertEquals(100L, response.indexStats["index-syncing"]!!.opsWritten)
    }
}
