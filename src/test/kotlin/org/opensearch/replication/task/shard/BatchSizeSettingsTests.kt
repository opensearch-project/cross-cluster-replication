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

import org.junit.After
import org.junit.Before
import org.junit.Test
import org.mockito.Mockito
import org.opensearch.Version
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Settings
import org.opensearch.index.IndexSettings
import org.opensearch.replication.ReplicationPlugin
import org.opensearch.replication.ReplicationSettings
import org.opensearch.test.ClusterServiceUtils
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.TestThreadPool
import java.util.concurrent.TimeUnit

class BatchSizeSettingsTests : OpenSearchTestCase() {

    private lateinit var clusterService: ClusterService
    private lateinit var replicationSettings: ReplicationSettings
    private lateinit var threadPool: TestThreadPool

    @Before
    fun setup() {
        threadPool = TestThreadPool("BatchSizeSettingsTest")
        
        // Create cluster settings with all replication plugin settings registered
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
                ReplicationPlugin.REPLICATION_FOLLOWER_RECOVERY_PARALLEL_CHUNKS
            )
        )
        
        // Create cluster state and cluster service with the proper settings
        val clusterState = ClusterState.builder(ClusterName.DEFAULT).build()
        clusterService = ClusterServiceUtils.createClusterService(clusterState, threadPool)
        
        // Replace the cluster settings in the cluster service
        val clusterServiceField = clusterService.javaClass.getDeclaredField("clusterSettings")
        clusterServiceField.isAccessible = true
        clusterServiceField.set(clusterService, clusterSettings)
        
        replicationSettings = ReplicationSettings(clusterService)
    }

    @Test
    fun `test fallback to cluster level when index level not set`() {
        // Create index settings without the index-level batch size setting
        val indexSettings = createIndexSettings(Settings.EMPTY)
        val batchSizeSettings = BatchSizeSettings(indexSettings, replicationSettings)

        // Should fallback to cluster-level setting
        assertEquals(50000, batchSizeSettings.getBatchSize()) // Default cluster value
        assertEquals("cluster-level", batchSizeSettings.getBatchSizeSource())
        assertFalse(batchSizeSettings.hasIndexLevelSetting())
    }

    @Test
    fun `test index level setting takes precedence`() {
        // Create index settings with index-level batch size
        val settings = Settings.builder()
            .put(ReplicationPlugin.REPLICATION_FOLLOWER_OPS_BATCH_SIZE_INDEX.key, 25000)
            .put(ReplicationPlugin.REPLICATION_FOLLOWER_OPS_BATCH_SIZE.key, 50000)
            .build()
        val indexSettings = createIndexSettings(settings)
        val batchSizeSettings = BatchSizeSettings(indexSettings, replicationSettings)

        // Should use index-level setting
        assertEquals(25000, batchSizeSettings.getBatchSize())
        assertEquals("index-level", batchSizeSettings.getBatchSizeSource())
        assertTrue(batchSizeSettings.hasIndexLevelSetting())
    }

    @Test
    fun `test dynamic batch size reduction`() {
        val indexSettings = createIndexSettings(Settings.EMPTY)
        val batchSizeSettings = BatchSizeSettings(indexSettings, replicationSettings)

        // Initial state
        assertEquals(50000, batchSizeSettings.getEffectiveBatchSize())
        assertFalse(batchSizeSettings.isDynamicallyReduced())

        // Reduce batch size
        batchSizeSettings.reduceBatchSize()
        assertEquals(25000, batchSizeSettings.getEffectiveBatchSize())
        assertTrue(batchSizeSettings.isDynamicallyReduced())

        // Reduce again
        batchSizeSettings.reduceBatchSize()
        assertEquals(12500, batchSizeSettings.getEffectiveBatchSize())
        assertTrue(batchSizeSettings.isDynamicallyReduced())

        // Reset to original
        batchSizeSettings.resetBatchSize()
        assertEquals(50000, batchSizeSettings.getEffectiveBatchSize())
        assertFalse(batchSizeSettings.isDynamicallyReduced())
    }

    @Test
    fun `test minimum batch size limit`() {
        val indexSettings = createIndexSettings(Settings.EMPTY)
        val batchSizeSettings = BatchSizeSettings(indexSettings, replicationSettings)

        // Reduce multiple times to hit minimum
        repeat(20) { batchSizeSettings.reduceBatchSize() }

        // Should not go below minimum (16)
        assertTrue(batchSizeSettings.getEffectiveBatchSize() >= 16)
    }

    @Test
    fun `test reset when not dynamically reduced does nothing`() {
        val indexSettings = createIndexSettings(Settings.EMPTY)
        val batchSizeSettings = BatchSizeSettings(indexSettings, replicationSettings)

        val originalSize = batchSizeSettings.getEffectiveBatchSize()
        
        // Reset without reducing first
        batchSizeSettings.resetBatchSize()
        
        // Should remain unchanged
        assertEquals(originalSize, batchSizeSettings.getEffectiveBatchSize())
        assertFalse(batchSizeSettings.isDynamicallyReduced())
    }

    @Test
    fun `test index level setting with dynamic reduction`() {
        // Create index settings with custom batch size
        val settings = Settings.builder()
            .put(ReplicationPlugin.REPLICATION_FOLLOWER_OPS_BATCH_SIZE_INDEX.key, 10000)
            .put(ReplicationPlugin.REPLICATION_FOLLOWER_OPS_BATCH_SIZE.key, 50000)
            .build()
        val indexSettings = createIndexSettings(settings)
        val batchSizeSettings = BatchSizeSettings(indexSettings, replicationSettings)

        // Initial state uses index-level setting
        assertEquals(10000, batchSizeSettings.getEffectiveBatchSize())

        // Reduce batch size
        batchSizeSettings.reduceBatchSize()
        assertEquals(5000, batchSizeSettings.getEffectiveBatchSize())

        // Reset should go back to index-level setting
        batchSizeSettings.resetBatchSize()
        assertEquals(10000, batchSizeSettings.getEffectiveBatchSize())
    }

    // Additional comprehensive test cases for 2GB limit handling and edge cases

    @Test
    fun `test batch size reduction with very small initial size`() {
        // Test with small initial batch size
        val settings = Settings.builder()
            .put(ReplicationPlugin.REPLICATION_FOLLOWER_OPS_BATCH_SIZE_INDEX.key, 32)
            .build()
        val indexSettings = createIndexSettings(settings)
        val batchSizeSettings = BatchSizeSettings(indexSettings, replicationSettings)

        assertEquals(32, batchSizeSettings.getEffectiveBatchSize())
        
        // First reduction: 32 -> 16 (minimum)
        batchSizeSettings.reduceBatchSize()
        assertEquals(16, batchSizeSettings.getEffectiveBatchSize())
        
        // Second reduction: should stay at minimum (16)
        batchSizeSettings.reduceBatchSize()
        assertEquals(16, batchSizeSettings.getEffectiveBatchSize())
        
        // Reset should go back to original
        batchSizeSettings.resetBatchSize()
        assertEquals(32, batchSizeSettings.getEffectiveBatchSize())
    }

    @Test
    fun `test batch size reduction with large initial size`() {
        // Test with large initial batch size
        val settings = Settings.builder()
            .put(ReplicationPlugin.REPLICATION_FOLLOWER_OPS_BATCH_SIZE_INDEX.key, 100000)
            .build()
        val indexSettings = createIndexSettings(settings)
        val batchSizeSettings = BatchSizeSettings(indexSettings, replicationSettings)

        assertEquals(100000, batchSizeSettings.getEffectiveBatchSize())
        
        // Progressive reductions
        batchSizeSettings.reduceBatchSize() // 100000 -> 50000
        assertEquals(50000, batchSizeSettings.getEffectiveBatchSize())
        
        batchSizeSettings.reduceBatchSize() // 50000 -> 25000
        assertEquals(25000, batchSizeSettings.getEffectiveBatchSize())
        
        batchSizeSettings.reduceBatchSize() // 25000 -> 12500
        assertEquals(12500, batchSizeSettings.getEffectiveBatchSize())
        
        batchSizeSettings.reduceBatchSize() // 12500 -> 6250
        assertEquals(6250, batchSizeSettings.getEffectiveBatchSize())
        
        // Reset should go back to original large size
        batchSizeSettings.resetBatchSize()
        assertEquals(100000, batchSizeSettings.getEffectiveBatchSize())
        assertFalse(batchSizeSettings.isDynamicallyReduced())
    }

    @Test
    fun `test concurrent batch size operations`() {
        val indexSettings = createIndexSettings(Settings.EMPTY)
        val batchSizeSettings = BatchSizeSettings(indexSettings, replicationSettings)

        // Test thread safety of batch size operations
        val threads = mutableListOf<Thread>()
        
        // Create multiple threads that reduce batch size
        repeat(5) {
            val thread = Thread {
                batchSizeSettings.reduceBatchSize()
            }
            threads.add(thread)
        }
        
        // Start all threads
        threads.forEach { it.start() }
        
        // Wait for all threads to complete
        threads.forEach { it.join() }
        
        // Verify that batch size was reduced 5 times: 50000 -> 1562
        assertTrue(batchSizeSettings.isDynamicallyReduced())
        assertEquals(1562, batchSizeSettings.getEffectiveBatchSize())
    }

    @Test
    fun `test batch size state consistency`() {
        val indexSettings = createIndexSettings(Settings.EMPTY)
        val batchSizeSettings = BatchSizeSettings(indexSettings, replicationSettings)

        // Initial state
        assertFalse(batchSizeSettings.isDynamicallyReduced())
        assertEquals(batchSizeSettings.getBatchSize(), batchSizeSettings.getEffectiveBatchSize())
        
        // After reduction
        batchSizeSettings.reduceBatchSize()
        assertTrue(batchSizeSettings.isDynamicallyReduced())
        assertNotEquals(batchSizeSettings.getBatchSize(), batchSizeSettings.getEffectiveBatchSize())
        assertTrue(batchSizeSettings.getEffectiveBatchSize() < batchSizeSettings.getBatchSize())
        
        // After reset
        batchSizeSettings.resetBatchSize()
        assertFalse(batchSizeSettings.isDynamicallyReduced())
        assertEquals(batchSizeSettings.getBatchSize(), batchSizeSettings.getEffectiveBatchSize())
    }

    @Test
    fun `test batch size source tracking`() {
        // Test cluster-level source
        val clusterOnlySettings = createIndexSettings(Settings.EMPTY)
        val clusterBatchSettings = BatchSizeSettings(clusterOnlySettings, replicationSettings)
        
        assertEquals("cluster-level", clusterBatchSettings.getBatchSizeSource())
        assertFalse(clusterBatchSettings.hasIndexLevelSetting())
        
        // Test index-level source
        val indexSettings = Settings.builder()
            .put(ReplicationPlugin.REPLICATION_FOLLOWER_OPS_BATCH_SIZE_INDEX.key, 30000)
            .build()
        val indexOnlySettings = createIndexSettings(indexSettings)
        val indexBatchSettings = BatchSizeSettings(indexOnlySettings, replicationSettings)
        
        assertEquals("index-level", indexBatchSettings.getBatchSizeSource())
        assertTrue(indexBatchSettings.hasIndexLevelSetting())
        
        // Source should remain consistent even after dynamic changes
        indexBatchSettings.reduceBatchSize()
        assertEquals("index-level", indexBatchSettings.getBatchSizeSource())
        assertTrue(indexBatchSettings.hasIndexLevelSetting())
    }

    @Test
    fun `test edge case with minimum batch size`() {
        // Test behavior when starting at minimum
        val settings = Settings.builder()
            .put(ReplicationPlugin.REPLICATION_FOLLOWER_OPS_BATCH_SIZE_INDEX.key, 16) // Minimum
            .build()
        val indexSettings = createIndexSettings(settings)
        val batchSizeSettings = BatchSizeSettings(indexSettings, replicationSettings)

        assertEquals(16, batchSizeSettings.getEffectiveBatchSize())
        assertFalse(batchSizeSettings.isDynamicallyReduced())
        
        // Reduction should still work but stay at minimum
        batchSizeSettings.reduceBatchSize()
        assertEquals(16, batchSizeSettings.getEffectiveBatchSize())
        assertTrue(batchSizeSettings.isDynamicallyReduced())
        
        // Multiple reductions should stay at minimum
        repeat(5) {
            batchSizeSettings.reduceBatchSize()
            assertEquals(16, batchSizeSettings.getEffectiveBatchSize())
            assertTrue(batchSizeSettings.isDynamicallyReduced())
        }
        
        // Reset should work normally
        batchSizeSettings.resetBatchSize()
        assertEquals(16, batchSizeSettings.getEffectiveBatchSize())
        assertFalse(batchSizeSettings.isDynamicallyReduced())
    }

    @Test
    fun `test multiple reset calls`() {
        val indexSettings = createIndexSettings(Settings.EMPTY)
        val batchSizeSettings = BatchSizeSettings(indexSettings, replicationSettings)

        val originalSize = batchSizeSettings.getEffectiveBatchSize()
        
        // Reduce batch size
        batchSizeSettings.reduceBatchSize()
        assertTrue(batchSizeSettings.isDynamicallyReduced())
        
        // Multiple reset calls should be safe
        batchSizeSettings.resetBatchSize()
        assertFalse(batchSizeSettings.isDynamicallyReduced())
        assertEquals(originalSize, batchSizeSettings.getEffectiveBatchSize())
        
        // Additional reset calls should be no-op
        batchSizeSettings.resetBatchSize()
        batchSizeSettings.resetBatchSize()
        assertFalse(batchSizeSettings.isDynamicallyReduced())
        assertEquals(originalSize, batchSizeSettings.getEffectiveBatchSize())
    }

    @Test
    fun `test batch size reduction sequence for 2GB handling simulation`() {
        // Simulate the exact scenario from 2GB exception handling
        val settings = Settings.builder()
            .put(ReplicationPlugin.REPLICATION_FOLLOWER_OPS_BATCH_SIZE_INDEX.key, 1000)
            .build()
        val indexSettings = createIndexSettings(settings)
        val batchSizeSettings = BatchSizeSettings(indexSettings, replicationSettings)

        // Initial state
        assertEquals(1000, batchSizeSettings.getEffectiveBatchSize())
        assertEquals("index-level", batchSizeSettings.getBatchSizeSource())
        
        // Simulate multiple 2GB exceptions requiring progressive reduction
        val reductionSequence = mutableListOf<Int>()
        
        // First 2GB exception
        batchSizeSettings.reduceBatchSize()
        reductionSequence.add(batchSizeSettings.getEffectiveBatchSize())
        
        // Second 2GB exception
        batchSizeSettings.reduceBatchSize()
        reductionSequence.add(batchSizeSettings.getEffectiveBatchSize())
        
        // Third 2GB exception
        batchSizeSettings.reduceBatchSize()
        reductionSequence.add(batchSizeSettings.getEffectiveBatchSize())
        
        // Verify reduction sequence: 1000 -> 500 -> 250 -> 125
        assertEquals(listOf(500, 250, 125), reductionSequence)
        assertTrue(batchSizeSettings.isDynamicallyReduced())
        
        // Simulate successful operation - reset batch size
        batchSizeSettings.resetBatchSize()
        assertEquals(1000, batchSizeSettings.getEffectiveBatchSize())
        assertFalse(batchSizeSettings.isDynamicallyReduced())
    }

    private fun createIndexSettings(settings: Settings): IndexSettings {
        val indexMetadata = org.opensearch.cluster.metadata.IndexMetadata.builder("test-index")
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
            // Close cluster service first to stop all background tasks
            clusterService.close()
        } catch (e: Exception) {
            logger.warn("Exception during cluster service cleanup", e)
        }
        
        try {
            // Shutdown thread pool gracefully
            threadPool.shutdown()
            if (!threadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn("Thread pool did not terminate gracefully, forcing shutdown")
                threadPool.shutdownNow()
                // Wait a bit more for forced shutdown
                if (!threadPool.awaitTermination(2, TimeUnit.SECONDS)) {
                    logger.warn("Thread pool did not terminate even after forced shutdown")
                }
            }
        } catch (e: Exception) {
            logger.warn("Exception during thread pool cleanup", e)
            try {
                threadPool.shutdownNow()
            } catch (e2: Exception) {
                logger.warn("Exception during forced thread pool shutdown", e2)
            }
        }
    }

    override fun tearDown() {
        super.tearDown()
    }
}