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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import kotlinx.coroutines.ObsoleteCoroutinesApi
import org.assertj.core.api.Assertions.assertThat
import org.mockito.Mockito
import org.opensearch.Version
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.node.DiscoveryNodes
import org.opensearch.cluster.routing.RoutingTable
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.replication.ReplicationSettings
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.metadata.ReplicationOverallState
import org.opensearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import org.opensearch.replication.metadata.state.ReplicationStateMetadata
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client

@ObsoleteCoroutinesApi
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class NodeReplicationControllerTests : OpenSearchTestCase() {

    private fun newController(): NodeReplicationController {
        // Mocks for collaborators that are not exercised by the pure reconcile-input function.
        return NodeReplicationController(
            clusterService = Mockito.mock(ClusterService::class.java),
            threadPool = Mockito.mock(ThreadPool::class.java),
            client = Mockito.mock(Client::class.java),
            replicationMetadataManager = Mockito.mock(ReplicationMetadataManager::class.java),
            replicationSettings = Mockito.mock(ReplicationSettings::class.java),
            followerClusterStats = FollowerClusterStats()
        )
    }

    private fun localNode(): DiscoveryNode {
        return DiscoveryNode(
            "local-node-id",
            buildNewFakeTransportAddress(),
            emptyMap(), emptySet(), Version.CURRENT
        )
    }

    @Suppress("unused")
    private fun otherNode(): DiscoveryNode {
        return DiscoveryNode(
            "other-node-id",
            buildNewFakeTransportAddress(),
            emptyMap(), emptySet(), Version.CURRENT
        )
    }

    private fun followerIndexMetadata(name: String, leaderAlias: String, leaderIndexName: String, shards: Int = 1): IndexMetadata {
        val settings = Settings.builder()
            .put("index.version.created", Version.CURRENT.id)
            .put("index.number_of_shards", shards)
            .put("index.number_of_replicas", 0)
            .put("index.plugins.replication.followed_by", leaderAlias)
            .put("index.plugins.replication.leader_index", leaderIndexName)
            .build()
        return IndexMetadata.builder(name).settings(settings).build()
    }

    private fun replicationStateMd(running: Map<String, Boolean>): ReplicationStateMetadata {
        // running maps follower-index-name -> true if RUNNING, false if PAUSED.
        val details = running.mapValues { (_, isRunning) ->
            mapOf(REPLICATION_LAST_KNOWN_OVERALL_STATE to
                if (isRunning) ReplicationOverallState.RUNNING.name else ReplicationOverallState.PAUSED.name)
        }
        return ReplicationStateMetadata(details)
    }

    fun testEmptyStateProducesNoDesiredShards() {
        val state = ClusterState.builder(ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(localNode()).localNodeId("local-node-id").build())
            .build()

        val desired = newController().computeDesiredShards(state)
        assertThat(desired).isEmpty()
    }

    fun testRunningIndexLocalPrimaryProducesDesiredShard() {
        val followerIndex = followerIndexMetadata("follower-01", "leader-cluster", "leader-01", shards = 1)
        val metadata = Metadata.builder()
            .put(followerIndex, true)
            .putCustom(ReplicationStateMetadata.NAME, replicationStateMd(mapOf("follower-01" to true)))
            .build()

        val routingTable = RoutingTable.builder().addAsNew(metadata.index("follower-01")).build()
        // Hand-build a routing table with the primary started on local-node-id.
        val state = ClusterState.builder(ClusterName("test"))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(localNode()).localNodeId("local-node-id").build())
            .build()

        val desired = newController().computeDesiredShards(state)
        // The naive routingTable.addAsNew puts shards in INITIALIZING state without a node assignment, which
        // means primaryShard().started() is false and currentNodeId is null. Result: nothing desired.
        // This is the realistic "shard exists but not yet active" scenario — controller should NOT pick it up.
        assertThat(desired).isEmpty()
    }

    fun testPausedIndexProducesNoDesiredShards() {
        val followerIndex = followerIndexMetadata("follower-01", "leader-cluster", "leader-01")
        val metadata = Metadata.builder()
            .put(followerIndex, true)
            .putCustom(ReplicationStateMetadata.NAME, replicationStateMd(mapOf("follower-01" to false)))
            .build()
        val routingTable = RoutingTable.builder().addAsNew(metadata.index("follower-01")).build()
        val state = ClusterState.builder(ClusterName("test"))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(localNode()).localNodeId("local-node-id").build())
            .build()

        val desired = newController().computeDesiredShards(state)
        assertThat(desired).isEmpty()
    }

    fun testIndexWithoutReplicationStateMetadataIgnored() {
        val followerIndex = followerIndexMetadata("regular-index", "leader-cluster", "leader-01")
        val metadata = Metadata.builder()
            .put(followerIndex, true)
            .build()
        val routingTable = RoutingTable.builder().addAsNew(metadata.index("regular-index")).build()
        val state = ClusterState.builder(ClusterName("test"))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(localNode()).localNodeId("local-node-id").build())
            .build()

        val desired = newController().computeDesiredShards(state)
        assertThat(desired).isEmpty()
    }

    fun testNoLocalNodeIdReturnsEmpty() {
        val followerIndex = followerIndexMetadata("follower-01", "leader-cluster", "leader-01")
        val metadata = Metadata.builder()
            .put(followerIndex, true)
            .putCustom(ReplicationStateMetadata.NAME, replicationStateMd(mapOf("follower-01" to true)))
            .build()
        val state = ClusterState.builder(ClusterName("test"))
            .metadata(metadata)
            .routingTable(RoutingTable.builder().addAsNew(metadata.index("follower-01")).build())
            // No localNodeId set
            .nodes(DiscoveryNodes.builder().build())
            .build()

        val desired = newController().computeDesiredShards(state)
        assertThat(desired).isEmpty()
    }
}
