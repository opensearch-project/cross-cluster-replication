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

package org.opensearch.replication.repository

import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import org.apache.lucene.index.IndexCommit
import org.opensearch.Version
import org.opensearch.action.support.single.shard.SingleShardRequest
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.service.ClusterService
import org.opensearch.core.index.shard.ShardId
import org.opensearch.common.concurrent.GatedCloseable
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException
import org.opensearch.index.shard.IndexShard
import org.opensearch.index.store.Store
import org.opensearch.indices.IndicesService
import org.opensearch.replication.ReplicationPlugin
import org.opensearch.replication.action.repository.GetStoreMetadataRequest
import org.opensearch.replication.action.repository.RemoteClusterRepositoryRequest
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.node.NodeClient

class RemoteClusterRestoreLeaderServiceTests : OpenSearchTestCase() {

    /**
     * Test double that skips the real shard/safe-commit/retention-lease machinery of constructRestoreContext
     * and hands back a RestoreContext built from mocks, so the admission-control and eviction logic can be
     * exercised in isolation. Each returned context wraps [commit] so we can assert it is closed on release.
     */
    private class TestableService(
            indicesService: IndicesService,
            nodeClient: NodeClient,
            threadPool: ThreadPool,
            clusterService: ClusterService,
            private val commit: GatedCloseable<IndexCommit>,
    ) : RemoteClusterRestoreLeaderService(indicesService, nodeClient, threadPool, clusterService) {

        override fun <T : SingleShardRequest<T>?> constructRestoreContext(
                restoreUUID: String, request: RemoteClusterRepositoryRequest<T>): RestoreContext {
            // followerShardId = null so eviction skips the (transport) retention-lease removal path.
            return RestoreContext(restoreUUID, mock<IndexShard>(), commit, Store.MetadataSnapshot.EMPTY, 0L, "", null)
        }
    }

    private fun buildService(maxRecoveries: Int, idleTimeout: TimeValue, threadPool: ThreadPool,
                             commit: GatedCloseable<IndexCommit> = mock()): TestableService {
        val settings = Settings.builder()
                .put(ReplicationPlugin.REPLICATION_LEADER_RESTORE_MAX_CONCURRENT_RECOVERIES.key, maxRecoveries)
                .put(ReplicationPlugin.REPLICATION_LEADER_RESTORE_SESSION_IDLE_TIMEOUT.key, idleTimeout)
                .build()
        val clusterSettings = ClusterSettings(settings, setOf(
                ReplicationPlugin.REPLICATION_LEADER_RESTORE_MAX_CONCURRENT_RECOVERIES,
                ReplicationPlugin.REPLICATION_LEADER_RESTORE_SESSION_IDLE_TIMEOUT))
        val clusterService = mock<ClusterService>()
        whenever(clusterService.clusterSettings).thenReturn(clusterSettings)
        return TestableService(mock(), mock(), threadPool, clusterService, commit)
    }

    // GetStoreMetadataRequest is final (not mockable); its fields are unused here since the overridden
    // constructRestoreContext ignores the request, so placeholder values are fine.
    private fun request(): GetStoreMetadataRequest {
        val shardId = ShardId("leader-index", "_na_", 0)
        val node = DiscoveryNode("node", buildNewFakeTransportAddress(), Version.CURRENT)
        return GetStoreMetadataRequest("restore-uuid", node, shardId, "follower", shardId)
    }

    fun `test addLeaderClusterRestore rejects new sessions beyond the per-node cap`() {
        val threadPool = mock<ThreadPool>()
        whenever(threadPool.relativeTimeInMillis()).thenReturn(0L)
        val service = buildService(maxRecoveries = 2, idleTimeout = TimeValue.timeValueMinutes(5), threadPool = threadPool)

        service.addLeaderClusterRestore("uuid-1", request())
        service.addLeaderClusterRestore("uuid-2", request())
        assertEquals(2, service.ongoingRestoreCount())

        val ex = expectThrows(OpenSearchRejectedExecutionException::class.java) {
            service.addLeaderClusterRestore("uuid-3", request())
        }
        assertTrue(ex.message!!.contains("capacity"))
        assertEquals(2, service.ongoingRestoreCount())
    }

    fun `test addLeaderClusterRestore is idempotent and does not consume a slot for an existing session`() {
        val threadPool = mock<ThreadPool>()
        whenever(threadPool.relativeTimeInMillis()).thenReturn(0L)
        val service = buildService(maxRecoveries = 1, idleTimeout = TimeValue.timeValueMinutes(5), threadPool = threadPool)

        val first = service.addLeaderClusterRestore("uuid-1", request())
        val again = service.addLeaderClusterRestore("uuid-1", request())

        assertSame(first, again)
        assertEquals(1, service.ongoingRestoreCount())
    }

    fun `test evictIdleRestores drops idle sessions and releases their resources`() {
        val commit = mock<GatedCloseable<IndexCommit>>()
        val threadPool = mock<ThreadPool>()
        // touch on add sees t=0; the eviction sweep sees t=60s, past the 30s idle window.
        whenever(threadPool.relativeTimeInMillis()).thenReturn(0L, 60_000L)
        val service = buildService(maxRecoveries = 4, idleTimeout = TimeValue.timeValueSeconds(30),
                threadPool = threadPool, commit = commit)

        service.addLeaderClusterRestore("uuid-1", request())
        assertEquals(1, service.ongoingRestoreCount())

        service.evictIdleRestores()

        assertEquals(0, service.ongoingRestoreCount())
        // Dropping the base ref runs closeInternal, which closes the safe index commit.
        verify(commit).close()
    }

    fun `test evictIdleRestores keeps sessions within the idle window`() {
        val commit = mock<GatedCloseable<IndexCommit>>()
        val threadPool = mock<ThreadPool>()
        // touch on add sees t=0; the sweep sees t=10s, still within the 30s idle window.
        whenever(threadPool.relativeTimeInMillis()).thenReturn(0L, 10_000L)
        val service = buildService(maxRecoveries = 4, idleTimeout = TimeValue.timeValueSeconds(30),
                threadPool = threadPool, commit = commit)

        service.addLeaderClusterRestore("uuid-1", request())
        service.evictIdleRestores()

        assertEquals(1, service.ongoingRestoreCount())
    }
}
