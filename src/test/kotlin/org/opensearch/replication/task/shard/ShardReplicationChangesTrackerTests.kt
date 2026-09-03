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

import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import org.opensearch.Version
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Settings
import org.opensearch.core.index.shard.ShardId
import org.opensearch.index.IndexSettings
import org.opensearch.index.shard.IndexShard
import org.opensearch.replication.ReplicationPlugin
import org.opensearch.replication.ReplicationSettings
import org.opensearch.test.ClusterServiceUtils
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.TestThreadPool
import java.util.concurrent.TimeUnit

/**
 * Unit tests for [ShardReplicationChangesTracker], focused on the interaction between the dynamic
 * batch-size reduction (used to recover from the 2GB serialization limit) and the replay of
 * "missing" batches. A missing batch that failed at the 2GB limit must be re-sliced to the current
 * (reduced) effective batch size on retry; otherwise it is replayed at its original width forever
 * and never makes progress.
 */
class ShardReplicationChangesTrackerTests : OpenSearchTestCase() {

    private lateinit var clusterService: ClusterService
    private lateinit var replicationSettings: ReplicationSettings
    private lateinit var threadPool: TestThreadPool
    private lateinit var mockIndexShard: IndexShard

    @Before
    fun setup() {
        threadPool = TestThreadPool("ShardReplicationChangesTrackerTests")

        val clusterSettings = ClusterSettings(
            Settings.EMPTY,
            setOf(
                ReplicationPlugin.REPLICATION_FOLLOWER_CONCURRENT_READERS_PER_SHARD,
                ReplicationPlugin.REPLICATION_FOLLOWER_CONCURRENT_WRITERS_PER_SHARD,
                ReplicationPlugin.REPLICATION_FOLLOWER_OPS_BATCH_SIZE,
                ReplicationPlugin.REPLICATION_PARALLEL_READ_POLL_INTERVAL,
                ReplicationPlugin.REPLICATION_AUTOFOLLOW_REMOTE_INDICES_POLL_INTERVAL,
                ReplicationPlugin.REPLICATION_AUTOFOLLOW_REMOTE_INDICES_RETRY_POLL_INTERVAL,
                ReplicationPlugin.REPLICATION_METADATA_SYNC_INTERVAL,
                ReplicationPlugin.REPLICATION_RETENTION_LEASE_MAX_FAILURE_DURATION,
                ReplicationPlugin.REPLICATION_FOLLOWER_BLOCK_START,
                ReplicationPlugin.REPLICATION_AUTOFOLLOW_CONCURRENT_REPLICATION_JOBS_TRIGGER_SIZE,
                ReplicationPlugin.REPLICATION_REPLICATE_INDEX_DELETION,
                ReplicationPlugin.REPLICATION_FOLLOWER_RECOVERY_CHUNK_SIZE,
                ReplicationPlugin.REPLICATION_FOLLOWER_RECOVERY_PARALLEL_CHUNKS,
                ReplicationPlugin.REPLICATION_FOLLOWER_BULK_BATCH_SIZE,
                ReplicationPlugin.REPLICATION_FOLLOWER_BULK_POLL_TIMEOUT
            )
        )

        val clusterState = ClusterState.builder(ClusterName.DEFAULT).build()
        clusterService = ClusterServiceUtils.createClusterService(clusterState, threadPool)
        val clusterServiceField = clusterService.javaClass.getDeclaredField("clusterSettings")
        clusterServiceField.isAccessible = true
        clusterServiceField.set(clusterService, clusterSettings)
        replicationSettings = ReplicationSettings(clusterService)

        mockIndexShard = mock(IndexShard::class.java)
        `when`(mockIndexShard.shardId()).thenReturn(ShardId("test-index", "test-uuid", 0))
        `when`(mockIndexShard.localCheckpoint).thenReturn(0L)
        `when`(mockIndexShard.indexSettings()).thenReturn(createIndexSettings(Settings.EMPTY))
    }

    /**
     * Reproduces the wedge: a wide missing batch (created under parallel readers) that, after the
     * dynamic reducer shrinks the effective batch size to the floor, must be handed out in slices of
     * the reduced size rather than replayed at its original width.
     */
    @Test
    fun `missing batch is re-sliced to the reduced effective batch size`() = runBlocking {
        val tracker = ShardReplicationChangesTracker(mockIndexShard, replicationSettings)
        val batchSize = replicationSettings.batchSize.toLong() // default 50000

        // Carve batch A (1..batchSize) and mark it complete, advancing the observed leader checkpoint
        // well ahead so subsequent carves don't block.
        val a = tracker.requestBatchToFetch()
        assertEquals(Pair(1L, batchSize), a)
        tracker.updateBatchFetched(true, a.first, a.second, a.second, 4 * batchSize)

        // Carve B and C so that when B fails it is NOT the last-requested batch and therefore lands
        // in missingBatches (the parallel-reader path) at its full original width.
        val b = tracker.requestBatchToFetch()
        assertEquals(Pair(batchSize + 1, 2 * batchSize), b)
        val c = tracker.requestBatchToFetch()
        assertEquals(Pair(2 * batchSize + 1, 3 * batchSize), c)

        // B fails entirely (received nothing) -> queued as a full-width missing batch.
        tracker.updateBatchFetched(false, b.first, b.second, b.first - 1, -1)

        // Simulate repeated 2GB hits driving the effective batch size to the floor (MIN_OPS_BATCH_SIZE=16).
        repeat(20) { tracker.reduceBatchSize() }
        val reduced = tracker.batchSizeSettings().getEffectiveBatchSize().toLong()
        assertEquals(16L, reduced)

        // The next fetch must return only a `reduced`-wide slice of the missing batch, not the whole span.
        val slice1 = tracker.requestBatchToFetch()
        assertEquals(Pair(b.first, b.first + reduced - 1), slice1)

        // And the remainder must be preserved and handed out contiguously on the following fetch.
        val slice2 = tracker.requestBatchToFetch()
        assertEquals(Pair(b.first + reduced, b.first + 2 * reduced - 1), slice2)
    }

    private fun createIndexSettings(settings: Settings): IndexSettings {
        val indexMetadata = IndexMetadata.builder("test-index")
            .settings(Settings.builder()
                .put(settings)
                .put("index.version.created", Version.CURRENT)
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .build())
            .build()
        return IndexSettings(indexMetadata, Settings.EMPTY)
    }

    @After
    fun cleanup() {
        try {
            clusterService.close()
        } catch (e: Exception) {
            logger.warn("Exception during cluster service cleanup", e)
        }
        try {
            threadPool.shutdown()
            if (!threadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                threadPool.shutdownNow()
            }
        } catch (e: Exception) {
            threadPool.shutdownNow()
        }
    }

    override fun tearDown() {
        super.tearDown()
    }
}
