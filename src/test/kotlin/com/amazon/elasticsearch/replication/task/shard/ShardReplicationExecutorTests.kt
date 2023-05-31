package com.amazon.elasticsearch.replication.task.shard

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import org.junit.Assert
import org.junit.Before
import org.mockito.Mockito
import com.amazon.elasticsearch.replication.ReplicationSettings
import com.amazon.elasticsearch.replication.metadata.ReplicationMetadataManager
import com.amazon.elasticsearch.replication.metadata.store.ReplicationMetadataStore
import com.amazon.elasticsearch.replication.task.index.*
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.cluster.metadata.Metadata
import org.elasticsearch.cluster.routing.IndexRoutingTable
import org.elasticsearch.cluster.routing.RecoverySource
import org.elasticsearch.cluster.routing.RoutingTable
import org.elasticsearch.cluster.routing.ShardRouting
import org.elasticsearch.cluster.routing.ShardRoutingState
import org.elasticsearch.cluster.routing.TestShardRouting
import org.elasticsearch.cluster.routing.UnassignedInfo
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.index.Index
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.test.ClusterServiceUtils
import org.elasticsearch.test.ESTestCase
import org.elasticsearch.test.ESTestCase.settings
import org.elasticsearch.test.client.NoOpClient
import org.elasticsearch.threadpool.TestThreadPool
import org.elasticsearch.Version
import java.util.ArrayList
import java.util.concurrent.TimeUnit

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class ShardReplicationExecutorTests : ESTestCase() {

    companion object {
        var followerIndex = "follower-index"
        var remoteCluster = "remote-cluster"
    }

    private lateinit var shardReplicationExecutor: ShardReplicationExecutor

    private var threadPool = TestThreadPool("ShardExecutorTest")
    private var clusterService = ClusterServiceUtils.createClusterService(threadPool)

    @Before
    fun setup() {
        val spyClient = Mockito.spy(NoOpClient("testName"))
        val replicationMetadataManager = ReplicationMetadataManager(
            clusterService, spyClient,
            ReplicationMetadataStore(spyClient, clusterService, NamedXContentRegistry.EMPTY)
        )
        val followerStats = FollowerClusterStats()
        val followerShardId = ShardId("follower", "follower_uuid", 0)
        followerStats.stats[followerShardId] = FollowerShardMetric()

        val replicationSettings = Mockito.mock(ReplicationSettings::class.java)
        replicationSettings.metadataSyncInterval = TimeValue(100, TimeUnit.MILLISECONDS)
        shardReplicationExecutor = ShardReplicationExecutor(
            "test_executor",
            clusterService,
            threadPool,
            spyClient,
            replicationMetadataManager,
            replicationSettings,
            followerStats
        )
    }

    fun `getAssignment should not throw exception when no shard is present`() {
        val sId = ShardId(Index(followerIndex, "_na_"), 0)
        val params = ShardReplicationParams(remoteCluster, sId, sId)
        val clusterState = createClusterState(null, null)

        try {
            val assignment = shardReplicationExecutor.getAssignment(params, clusterState)
            Assert.assertEquals(null, assignment.executorNode)
        } catch (e: Exception) {
            // Validation should not throw an exception, so the test should fail if it reaches this line
            Assert.fail("Expected Exception should not be thrown")
        }
    }

    fun `getAssignment should return null if shard is present but is not active`() {
        val sId = ShardId(Index(followerIndex, "_na_"), 0)
        val params = ShardReplicationParams(remoteCluster, sId, sId)
        val unassignedShard = ShardRouting.newUnassigned(
            sId,
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null)
        )
        val clusterState = createClusterState(sId, unassignedShard)

        try {
            val assignment = shardReplicationExecutor.getAssignment(params, clusterState)
            Assert.assertEquals(null, assignment.executorNode)
        } catch (e: Exception) {
            // Validation should not throw an exception, so the test should fail if it reaches this line
            Assert.fail("Expected Exception should not be thrown")
        }
    }

    fun `getAssignment should return node when shard is present`() {
        val sId = ShardId(Index(followerIndex, "_na_"), 0)
        val params = ShardReplicationParams(remoteCluster, sId, sId)
        val initializingShard = TestShardRouting.newShardRouting(
            followerIndex,
            sId.id,
            "1",
            true,
            ShardRoutingState.INITIALIZING
        )
        val startedShard = initializingShard.moveToStarted()
        val clusterState = createClusterState(sId, startedShard)

        try {
            val assignment = shardReplicationExecutor.getAssignment(params, clusterState)
            Assert.assertEquals(initializingShard.currentNodeId(), assignment.executorNode)
        } catch (e: Exception) {
            // Validation should not throw an exception, so the test should fail if it reaches this line
            Assert.fail("Expected Exception should not be thrown")
        }
    }

    private fun createClusterState(shardId: ShardId?, shardRouting: ShardRouting?): ClusterState {
        val indices: MutableList<String> = ArrayList()
        indices.add(followerIndex)
        val metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(ReplicationMetadataStore.REPLICATION_CONFIG_SYSTEM_INDEX)
                    .settings(
                        settings(
                            Version.CURRENT
                        )
                    ).numberOfShards(1).numberOfReplicas(0)
            )
            .put(
                IndexMetadata.builder(followerIndex).settings(
                    settings(
                        Version.CURRENT
                    )
                ).numberOfShards(2).numberOfReplicas(0)
            )
            .build()

        val routingTableBuilder = RoutingTable.builder()
            .addAsNew(metadata.index(ReplicationMetadataStore.REPLICATION_CONFIG_SYSTEM_INDEX))
            .addAsNew(metadata.index(followerIndex))

        if (shardId != null) {
            routingTableBuilder.add(
                IndexRoutingTable.builder(shardId.index)
                    .addShard(shardRouting)
                    .build()
            )
        }

        return ClusterState.builder(clusterService.state())
            .routingTable(routingTableBuilder.build()).build()
    }
}