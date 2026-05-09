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

package org.opensearch.replication.action.resume

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.eq
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.never
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito
import org.opensearch.Version
import org.opensearch.action.admin.indices.stats.IndexShardStats
import org.opensearch.action.admin.indices.stats.IndicesStatsAction
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.action.admin.indices.stats.ShardStats
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.routing.IndexRoutingTable
import org.opensearch.cluster.routing.IndexShardRoutingTable
import org.opensearch.cluster.routing.RoutingTable
import org.opensearch.cluster.routing.ShardRouting
import org.opensearch.cluster.routing.ShardRoutingState
import org.opensearch.cluster.routing.TestShardRouting
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.core.index.Index
import org.opensearch.core.index.shard.ShardId
import org.opensearch.index.seqno.RetentionLeaseAlreadyExistsException
import org.opensearch.index.seqno.SeqNoStats
import org.opensearch.replication.action.index.block.IndexBlockUpdateType
import org.opensearch.replication.action.index.block.UpdateIndexBlockAction
import org.opensearch.replication.task.index.IndexReplicationParams
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.TestThreadPool
import org.opensearch.transport.client.AdminClient
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.IndicesAdminClient
import java.util.concurrent.TimeUnit

/**
 * Unit tests for [ForceResumeCoordinator].
 * The coordinator orchestrates: remove block → acquire leases → delete follower.
 * On failure it must clean up leases and re-add the block.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class ForceResumeCoordinatorTests : OpenSearchTestCase() {

    companion object {
        private const val FOLLOWER_INDEX = "follower-index"
        private const val LEADER_INDEX = "leader-index"
        private const val LEADER_ALIAS = "leader-cluster"
        private const val LEADER_INDEX_UUID = "leader-uuid"
        private const val FOLLOWER_INDEX_UUID = "follower-uuid"
        private const val CLUSTER_NAME = "follower-cluster"
        private const val CLUSTER_UUID = "follower-cluster-uuid"
    }

    private lateinit var threadPool: TestThreadPool
    private lateinit var client: Client
    private lateinit var remoteClient: Client
    private lateinit var clusterService: ClusterService
    private lateinit var adminClient: AdminClient
    private lateinit var indicesAdminClient: IndicesAdminClient

    private val leaderIndex = Index(LEADER_INDEX, LEADER_INDEX_UUID)
    private val followerIndex = Index(FOLLOWER_INDEX, FOLLOWER_INDEX_UUID)

    override fun setUp() {
        super.setUp()
        threadPool = TestThreadPool("ForceResumeCoordinatorTests")
        client = Mockito.mock(Client::class.java)
        remoteClient = Mockito.mock(Client::class.java)
        clusterService = Mockito.mock(ClusterService::class.java)
        adminClient = Mockito.mock(AdminClient::class.java)
        indicesAdminClient = Mockito.mock(IndicesAdminClient::class.java)

        // Wire up client.getRemoteClusterClient
        whenever(client.getRemoteClusterClient(anyString())).thenReturn(remoteClient)

        // Wire up threadPool for both clients
        whenever(client.threadPool()).thenReturn(threadPool)
        whenever(remoteClient.threadPool()).thenReturn(threadPool)

        // Wire up admin client chain for deleteFollowerIndex
        whenever(client.admin()).thenReturn(adminClient)
        whenever(adminClient.indices()).thenReturn(indicesAdminClient)

        // Stub indices delete to call the listener with acknowledged response
        doAnswer { invocation ->
            val listener = invocation.getArgument<ActionListener<AcknowledgedResponse>>(1)
            listener.onResponse(AcknowledgedResponse(true))
            null
        }.whenever(indicesAdminClient).delete(any(), any<ActionListener<AcknowledgedResponse>>())

        // Wire up clusterService basics
        whenever(clusterService.clusterName).thenReturn(ClusterName(CLUSTER_NAME))
    }

    override fun tearDown() {
        super.tearDown()
        org.opensearch.threadpool.ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS)
    }

    private fun buildParams(numShards: Int = 1): IndexReplicationParams {
        return IndexReplicationParams(LEADER_ALIAS, leaderIndex, FOLLOWER_INDEX)
    }

    // Builds a ClusterState with the follower index having the given number of shards
    private fun buildClusterState(numShards: Int): ClusterState {
        val indexMetadata = IndexMetadata.builder(FOLLOWER_INDEX)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, FOLLOWER_INDEX_UUID)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .build()

        val metadata = Metadata.builder()
            .put(indexMetadata, false)
            .clusterUUID(CLUSTER_UUID)
            .build()

        val indexRoutingTableBuilder = IndexRoutingTable.builder(indexMetadata.index)
        for (i in 0 until numShards) {
            val shardId = ShardId(indexMetadata.index, i)
            val shardRouting = TestShardRouting.newShardRouting(
                shardId, "node-1", true, ShardRoutingState.STARTED
            )
            indexRoutingTableBuilder.addShard(shardRouting)
        }

        val routingTable = RoutingTable.builder()
            .add(indexRoutingTableBuilder.build())
            .build()

        return ClusterState.builder(ClusterName(CLUSTER_NAME))
            .metadata(metadata)
            .routingTable(routingTable)
            .build()
    }

    /**
     * Stubs the client.execute() call to respond with AcknowledgedResponse for any action.
     * This covers removeIndexBlock, deleteFollowerIndex, and re-add block calls.
     */
    private fun stubClientExecuteAck() {
        doAnswer { invocation ->
            val listener = invocation.getArgument<ActionListener<ActionResponse>>(2)
            listener.onResponse(AcknowledgedResponse(true))
            null
        }.whenever(client).execute(any(), any(), any<ActionListener<ActionResponse>>())
    }

    /**
     * Stubs the remoteClient.execute() for IndicesStatsAction to return a given global checkpoint
     * for each shard.
     */
    private fun stubRemoteStatsResponse(numShards: Int, globalCheckpoint: Long) {
        doAnswer { invocation ->
            val listener = invocation.getArgument<ActionListener<ActionResponse>>(2)
            val shardStats = (0 until numShards).map { shardId ->
                val shardRouting = TestShardRouting.newShardRouting(
                    ShardId(leaderIndex, shardId), "leader-node", true, ShardRoutingState.STARTED
                )
                val seqNoStats = SeqNoStats(globalCheckpoint, globalCheckpoint, globalCheckpoint)
                Mockito.mock(ShardStats::class.java).also { ss ->
                    whenever(ss.shardRouting).thenReturn(shardRouting)
                    whenever(ss.seqNoStats).thenReturn(seqNoStats)
                }
            }.toTypedArray()

            val statsResponse = Mockito.mock(IndicesStatsResponse::class.java)
            whenever(statsResponse.shards).thenReturn(shardStats)
            listener.onResponse(statsResponse as ActionResponse)
            null
        }.whenever(remoteClient).execute(any(), any(), any<ActionListener<ActionResponse>>())
    }

    // ======================== Tests ========================

    fun `test ForceResumeResult is returned on success with single shard`() = runBlocking {
        val numShards = 1
        val globalCheckpoint = 42L
        val clusterState = buildClusterState(numShards)
        whenever(clusterService.state()).thenReturn(clusterState)

        stubClientExecuteAck()
        stubRemoteStatsResponse(numShards, globalCheckpoint)

        val coordinator = ForceResumeCoordinator(client, clusterService)
        val result = coordinator.executeForceResume(buildParams())

        assertThat(result.successful).isTrue()
        assertThat(result.followerIndex).isEqualTo(FOLLOWER_INDEX)
        assertThat(result.leaseAcquiredAtSeqNo).hasSize(numShards)
        // Lease should be acquired at globalCheckpoint + 1
        assertThat(result.leaseAcquiredAtSeqNo[0]).isEqualTo(globalCheckpoint + 1)
        assertThat(result.durationMillis).isGreaterThanOrEqualTo(0)
    }

    fun `test removeIndexBlock is called with REMOVE_BLOCK`() = runBlocking {
        val clusterState = buildClusterState(1)
        whenever(clusterService.state()).thenReturn(clusterState)

        stubClientExecuteAck()
        stubRemoteStatsResponse(1, 10L)

        val coordinator = ForceResumeCoordinator(client, clusterService)
        coordinator.executeForceResume(buildParams())

        // removeIndexBlock uses client.execute (suspendExecute) — at least 1 call
        verify(client, Mockito.atLeast(1)).execute(any(), any(), any<ActionListener<ActionResponse>>())
        // deleteFollowerIndex uses client.admin().indices().delete — verify that path was called
        verify(indicesAdminClient, Mockito.atLeast(1)).delete(any(), any<ActionListener<AcknowledgedResponse>>())
    }

    fun `test cleanup re-adds block when lease acquisition fails`() = runBlocking {
        val clusterState = buildClusterState(1)
        whenever(clusterService.state()).thenReturn(clusterState)

        // First call succeeds (removeIndexBlock), subsequent calls for cleanup also succeed
        stubClientExecuteAck()

        // Make remote stats call fail to simulate lease acquisition failure
        doAnswer { invocation ->
            val listener = invocation.getArgument<ActionListener<ActionResponse>>(2)
            listener.onFailure(RuntimeException("Stats call failed"))
            null
        }.whenever(remoteClient).execute(any(), any(), any<ActionListener<ActionResponse>>())

        val coordinator = ForceResumeCoordinator(client, clusterService)

        assertThatThrownBy {
            runBlocking { coordinator.executeForceResume(buildParams()) }
        }.isInstanceOf(RuntimeException::class.java)
            .hasMessageContaining("Stats call failed")

        // Verify that client.execute was called more than once (block removal + re-add on cleanup)
        verify(client, Mockito.atLeast(2)).execute(any(), any(), any<ActionListener<ActionResponse>>())
    }

    fun `test no shards means no leases acquired`() = runBlocking {
        // Build a cluster state where the follower index has no routing entry
        val metadata = Metadata.builder()
            .clusterUUID(CLUSTER_UUID)
            .build()
        val routingTable = RoutingTable.builder().build()
        val clusterState = ClusterState.builder(ClusterName(CLUSTER_NAME))
            .metadata(metadata)
            .routingTable(routingTable)
            .build()

        whenever(clusterService.state()).thenReturn(clusterState)
        stubClientExecuteAck()

        val coordinator = ForceResumeCoordinator(client, clusterService)
        val result = coordinator.executeForceResume(buildParams())

        assertThat(result.successful).isTrue()
        assertThat(result.leaseAcquiredAtSeqNo).isEmpty()
    }

    fun `test global checkpoint of zero results in lease at seqNo 1`() = runBlocking {
        val clusterState = buildClusterState(1)
        whenever(clusterService.state()).thenReturn(clusterState)

        stubClientExecuteAck()
        stubRemoteStatsResponse(1, 0L)

        val coordinator = ForceResumeCoordinator(client, clusterService)
        val result = coordinator.executeForceResume(buildParams())

        assertThat(result.leaseAcquiredAtSeqNo[0]).isEqualTo(1L)
    }

    fun `test removeIndexBlock failure propagates without cleanup`() = runBlocking {
        val clusterState = buildClusterState(1)
        whenever(clusterService.state()).thenReturn(clusterState)

        // Make the first client.execute call (removeIndexBlock) fail
        doAnswer { invocation ->
            val listener = invocation.getArgument<ActionListener<ActionResponse>>(2)
            listener.onFailure(RuntimeException("Block removal failed"))
            null
        }.whenever(client).execute(any(), any(), any<ActionListener<ActionResponse>>())

        val coordinator = ForceResumeCoordinator(client, clusterService)

        assertThatThrownBy {
            runBlocking { coordinator.executeForceResume(buildParams()) }
        }.isInstanceOf(RuntimeException::class.java)
            .hasMessageContaining("Block removal failed")

        // No remote calls should have been made since we failed at step 1
        verify(remoteClient, never()).execute(any(), any(), any<ActionListener<ActionResponse>>())
    }

    fun `test delete follower index failure triggers cleanup`() = runBlocking {
        val clusterState = buildClusterState(1)
        whenever(clusterService.state()).thenReturn(clusterState)

        // client.execute succeeds (for removeIndexBlock and re-add block in cleanup)
        stubClientExecuteAck()
        // remote stats succeeds (lease acquisition works)
        stubRemoteStatsResponse(1, 50L)

        // But delete follower index fails
        doAnswer { invocation ->
            val listener = invocation.getArgument<ActionListener<AcknowledgedResponse>>(1)
            listener.onFailure(RuntimeException("Delete index failed"))
            null
        }.whenever(indicesAdminClient).delete(any(), any<ActionListener<AcknowledgedResponse>>())

        val coordinator = ForceResumeCoordinator(client, clusterService)

        assertThatThrownBy {
            runBlocking { coordinator.executeForceResume(buildParams()) }
        }.isInstanceOf(RuntimeException::class.java)
            .hasMessageContaining("Delete index failed")

        // Cleanup should have re-added the block (at least 2 client.execute calls:
        // 1 for removeIndexBlock, 1 for re-add block in cleanup)
        verify(client, Mockito.atLeast(2)).execute(any(), any(), any<ActionListener<ActionResponse>>())
    }

    fun `test primary shard not found throws IllegalStateException`() = runBlocking {
        val clusterState = buildClusterState(1)
        whenever(clusterService.state()).thenReturn(clusterState)

        stubClientExecuteAck()

        // Return stats with no matching primary shard
        doAnswer { invocation ->
            val listener = invocation.getArgument<ActionListener<ActionResponse>>(2)
            val statsResponse = Mockito.mock(IndicesStatsResponse::class.java)
            whenever(statsResponse.shards).thenReturn(emptyArray())
            listener.onResponse(statsResponse as ActionResponse)
            null
        }.whenever(remoteClient).execute(any(), any(), any<ActionListener<ActionResponse>>())

        val coordinator = ForceResumeCoordinator(client, clusterService)

        assertThatThrownBy {
            runBlocking { coordinator.executeForceResume(buildParams()) }
        }.isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("Primary shard")
            .hasMessageContaining("not found")
    }

    fun `test multiple shards with different global checkpoints`() = runBlocking {
        val numShards = 3
        val clusterState = buildClusterState(numShards)
        whenever(clusterService.state()).thenReturn(clusterState)

        stubClientExecuteAck()

        // Return different global checkpoints per shard
        doAnswer { invocation ->
            val listener = invocation.getArgument<ActionListener<ActionResponse>>(2)
            val shardStats = (0 until numShards).map { shardId ->
                val shardRouting = TestShardRouting.newShardRouting(
                    ShardId(leaderIndex, shardId), "leader-node", true, ShardRoutingState.STARTED
                )
                val checkpoint = (shardId + 1) * 100L // 100, 200, 300
                val seqNoStats = SeqNoStats(checkpoint, checkpoint, checkpoint)
                Mockito.mock(ShardStats::class.java).also { ss ->
                    whenever(ss.shardRouting).thenReturn(shardRouting)
                    whenever(ss.seqNoStats).thenReturn(seqNoStats)
                }
            }.toTypedArray()

            val statsResponse = Mockito.mock(IndicesStatsResponse::class.java)
            whenever(statsResponse.shards).thenReturn(shardStats)
            listener.onResponse(statsResponse as ActionResponse)
            null
        }.whenever(remoteClient).execute(any(), any(), any<ActionListener<ActionResponse>>())

        val coordinator = ForceResumeCoordinator(client, clusterService)
        val result = coordinator.executeForceResume(buildParams())

        assertThat(result.successful).isTrue()
        assertThat(result.leaseAcquiredAtSeqNo).hasSize(numShards)
        assertThat(result.leaseAcquiredAtSeqNo[0]).isEqualTo(101L)
        assertThat(result.leaseAcquiredAtSeqNo[1]).isEqualTo(201L)
        assertThat(result.leaseAcquiredAtSeqNo[2]).isEqualTo(301L)
    }

}
