package org.opensearch.replication.task.shard

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.mockito.Mockito
import org.opensearch.Version
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.routing.*
import org.opensearch.common.unit.TimeValue
import org.opensearch.core.index.Index
import org.opensearch.core.index.shard.ShardId
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.replication.ReplicationSettings
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.metadata.store.ReplicationMetadataStore
import org.opensearch.replication.task.index.*
import org.opensearch.test.ClusterServiceUtils
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.TestThreadPool
import java.util.ArrayList
import java.util.concurrent.TimeUnit

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class ShardReplicationExecutorTests : OpenSearchTestCase() {
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
        val replicationMetadataManager =
            ReplicationMetadataManager(
                clusterService,
                spyClient,
                ReplicationMetadataStore(spyClient, clusterService, NamedXContentRegistry.EMPTY),
            )
        val followerStats = FollowerClusterStats()
        val followerShardId = ShardId("follower", "follower_uuid", 0)
        followerStats.stats[followerShardId] = FollowerShardMetric()

        val replicationSettings = Mockito.mock(ReplicationSettings::class.java)
        replicationSettings.metadataSyncInterval = TimeValue(100, TimeUnit.MILLISECONDS)
        shardReplicationExecutor =
            ShardReplicationExecutor(
                "test_executor",
                clusterService,
                threadPool,
                spyClient,
                replicationMetadataManager,
                replicationSettings,
                followerStats,
            )
    }

    @Test
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

    @Test
    fun `getAssignment should return null if shard is present but is not active`() {
        val sId = ShardId(Index(followerIndex, "_na_"), 0)
        val params = ShardReplicationParams(remoteCluster, sId, sId)
        val unassignedShard =
            ShardRouting.newUnassigned(
                sId,
                true,
                RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null),
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

    @Test
    fun `getAssignment should return node when shard is present`() {
        val sId = ShardId(Index(followerIndex, "_na_"), 0)
        val params = ShardReplicationParams(remoteCluster, sId, sId)
        val initializingShard =
            TestShardRouting.newShardRouting(
                followerIndex,
                sId.id,
                "1",
                true,
                ShardRoutingState.INITIALIZING,
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

    private fun createClusterState(
        shardId: ShardId?,
        shardRouting: ShardRouting?,
    ): ClusterState {
        val indices: MutableList<String> = ArrayList()
        indices.add(followerIndex)
        val metadata =
            Metadata
                .builder()
                .put(
                    IndexMetadata
                        .builder(ReplicationMetadataStore.REPLICATION_CONFIG_SYSTEM_INDEX)
                        .settings(
                            settings(Version.CURRENT),
                        ).numberOfShards(1)
                        .numberOfReplicas(0),
                ).put(
                    IndexMetadata
                        .builder(IndexReplicationTaskTests.followerIndex)
                        .settings(
                            settings(Version.CURRENT),
                        ).numberOfShards(2)
                        .numberOfReplicas(0),
                ).build()

        val routingTableBuilder =
            RoutingTable
                .builder()
                .addAsNew(metadata.index(ReplicationMetadataStore.REPLICATION_CONFIG_SYSTEM_INDEX))
                .addAsNew(metadata.index(followerIndex))

        if (shardId != null) {
            routingTableBuilder.add(
                IndexRoutingTable
                    .builder(shardId.index)
                    .addShard(shardRouting)
                    .build(),
            )
        }

        return ClusterState.builder(clusterService.state()).routingTable(routingTableBuilder.build()).build()
    }
}
