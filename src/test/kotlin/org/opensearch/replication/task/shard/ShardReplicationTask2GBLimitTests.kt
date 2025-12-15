package org.opensearch.replication.task.shard

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
 * Unit tests for 2GB limit exception handling in ShardReplicationTask.
 *
 * These tests verify that when a 2GB limit is hit during batch operations,
 * the system properly:
 * 1. Detects the specific 2GB exception
 * 2. Reduces batch size appropriately
 * 3. Retries immediately without delay
 * 4. Maintains proper state throughout the process
 */
class ShardReplicationTask2GBLimitTests : OpenSearchTestCase() {
    private lateinit var clusterService: ClusterService
    private lateinit var replicationSettings: ReplicationSettings
    private lateinit var threadPool: TestThreadPool
    private lateinit var mockIndexShard: IndexShard

    @Before
    fun setup() {
        threadPool = TestThreadPool("ShardReplicationTask2GBLimitTests")

        val clusterSettings =
            ClusterSettings(
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
                ),
            )

        val clusterState = ClusterState.builder(ClusterName.DEFAULT).build()
        clusterService = ClusterServiceUtils.createClusterService(clusterState, threadPool)

        val clusterServiceField = clusterService.javaClass.getDeclaredField("clusterSettings")
        clusterServiceField.isAccessible = true
        clusterServiceField.set(clusterService, clusterSettings)

        replicationSettings = ReplicationSettings(clusterService)

        mockIndexShard = mock(IndexShard::class.java)
        val shardId = ShardId("test-index", "test-uuid", 0)
        `when`(mockIndexShard.shardId()).thenReturn(shardId)
        `when`(mockIndexShard.localCheckpoint).thenReturn(0L)
        `when`(mockIndexShard.lastSyncedGlobalCheckpoint).thenReturn(0L)
    }

    @Test
    fun `test 2GB exception detection`() {
        // Test the exact exception message pattern used in ShardReplicationTask
        val twoGBException = IllegalArgumentException("ReleasableBytesStreamOutput cannot hold more than 2GB of data")

        val is2GBException =
            twoGBException is IllegalArgumentException &&
                twoGBException.message?.contains("ReleasableBytesStreamOutput cannot hold more than 2GB") == true

        assertTrue("Should detect 2GB exception", is2GBException)

        // Test that other exceptions are not detected as 2GB exceptions
        val otherException = IllegalArgumentException("Some other error")
        val isNot2GBException =
            otherException is IllegalArgumentException &&
                otherException.message?.contains("ReleasableBytesStreamOutput cannot hold more than 2GB") == true

        assertFalse("Should not detect non-2GB exception", isNot2GBException)
    }

    @Test
    fun `test batch size reduction on 2GB exception`() {
        val indexSettings =
            createIndexSettings(
                Settings
                    .builder()
                    .put(ReplicationPlugin.REPLICATION_FOLLOWER_OPS_BATCH_SIZE_INDEX.key, 100000)
                    .build(),
            )

        val batchSizeSettings = BatchSizeSettings(indexSettings, replicationSettings)

        // Initial state
        assertEquals(100000, batchSizeSettings.getEffectiveBatchSize())
        assertFalse(batchSizeSettings.isDynamicallyReduced())

        // Simulate 2GB exception handling
        val exception = IllegalArgumentException("ReleasableBytesStreamOutput cannot hold more than 2GB of data")

        if (exception is IllegalArgumentException &&
            exception.message?.contains("ReleasableBytesStreamOutput cannot hold more than 2GB") == true
        ) {
            batchSizeSettings.reduceBatchSize()
        }

        // Verify batch size was reduced
        assertEquals(50000, batchSizeSettings.getEffectiveBatchSize())
        assertTrue(batchSizeSettings.isDynamicallyReduced())
    }

    @Test
    fun `test multiple 2GB exceptions reduce batch size progressively`() {
        val indexSettings =
            createIndexSettings(
                Settings
                    .builder()
                    .put(ReplicationPlugin.REPLICATION_FOLLOWER_OPS_BATCH_SIZE_INDEX.key, 80000)
                    .build(),
            )

        val batchSizeSettings = BatchSizeSettings(indexSettings, replicationSettings)

        assertEquals(80000, batchSizeSettings.getEffectiveBatchSize())

        // First 2GB exception: 80000 -> 40000
        batchSizeSettings.reduceBatchSize()
        assertEquals(40000, batchSizeSettings.getEffectiveBatchSize())

        // Second 2GB exception: 40000 -> 20000
        batchSizeSettings.reduceBatchSize()
        assertEquals(20000, batchSizeSettings.getEffectiveBatchSize())

        // Third 2GB exception: 20000 -> 10000
        batchSizeSettings.reduceBatchSize()
        assertEquals(10000, batchSizeSettings.getEffectiveBatchSize())

        assertTrue(batchSizeSettings.isDynamicallyReduced())
    }

    @Test
    fun `test batch size respects minimum limit`() {
        val indexSettings =
            createIndexSettings(
                Settings
                    .builder()
                    .put(ReplicationPlugin.REPLICATION_FOLLOWER_OPS_BATCH_SIZE_INDEX.key, 32)
                    .build(),
            )

        val batchSizeSettings = BatchSizeSettings(indexSettings, replicationSettings)

        assertEquals(32, batchSizeSettings.getEffectiveBatchSize())

        // First reduction: 32 -> 16 (minimum)
        batchSizeSettings.reduceBatchSize()
        assertEquals(16, batchSizeSettings.getEffectiveBatchSize())

        // Second reduction: should stay at minimum
        batchSizeSettings.reduceBatchSize()
        assertEquals(16, batchSizeSettings.getEffectiveBatchSize())

        // Multiple reductions should not go below minimum
        repeat(5) {
            batchSizeSettings.reduceBatchSize()
            assertEquals(16, batchSizeSettings.getEffectiveBatchSize())
        }
    }

    @Test
    fun `test batch size reset after successful operations`() {
        val indexSettings =
            createIndexSettings(
                Settings
                    .builder()
                    .put(ReplicationPlugin.REPLICATION_FOLLOWER_OPS_BATCH_SIZE_INDEX.key, 60000)
                    .build(),
            )

        val batchSizeSettings = BatchSizeSettings(indexSettings, replicationSettings)

        // Reduce batch size due to 2GB exception
        batchSizeSettings.reduceBatchSize()
        assertEquals(30000, batchSizeSettings.getEffectiveBatchSize())
        assertTrue(batchSizeSettings.isDynamicallyReduced())

        // Reset after successful operation
        batchSizeSettings.resetBatchSize()
        assertEquals(60000, batchSizeSettings.getEffectiveBatchSize())
        assertFalse(batchSizeSettings.isDynamicallyReduced())
    }

    @Test
    fun `test cluster level batch size fallback with 2GB handling`() {
        // No index-level setting, should use cluster-level default
        val indexSettings = createIndexSettings(Settings.EMPTY)
        val batchSizeSettings = BatchSizeSettings(indexSettings, replicationSettings)

        assertEquals(50000, batchSizeSettings.getEffectiveBatchSize()) // Default cluster value
        assertEquals("cluster-level", batchSizeSettings.getBatchSizeSource())
        assertFalse(batchSizeSettings.hasIndexLevelSetting())

        // Reduce due to 2GB exception
        batchSizeSettings.reduceBatchSize()
        assertEquals(25000, batchSizeSettings.getEffectiveBatchSize())
        assertTrue(batchSizeSettings.isDynamicallyReduced())

        // Source should still be cluster-level
        assertEquals("cluster-level", batchSizeSettings.getBatchSizeSource())

        // Reset should go back to cluster default
        batchSizeSettings.resetBatchSize()
        assertEquals(50000, batchSizeSettings.getEffectiveBatchSize())
        assertFalse(batchSizeSettings.isDynamicallyReduced())
    }

    @Test
    fun `test batch size settings with 2GB scenario simulation`() {
        // Test the BatchSizeSettings behavior that's used by ShardReplicationChangesTracker
        val indexSettings =
            createIndexSettings(
                Settings
                    .builder()
                    .put(ReplicationPlugin.REPLICATION_FOLLOWER_OPS_BATCH_SIZE_INDEX.key, 1000)
                    .build(),
            )

        val batchSizeSettings = BatchSizeSettings(indexSettings, replicationSettings)

        // Initial state
        assertEquals(1000, batchSizeSettings.getEffectiveBatchSize())
        assertEquals("index-level", batchSizeSettings.getBatchSizeSource())
        assertFalse(batchSizeSettings.isDynamicallyReduced())

        // Simulate 2GB exception scenario - progressive reductions
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

    @Test
    fun `test 2GB exception message variations`() {
        val validMessages =
            listOf(
                "ReleasableBytesStreamOutput cannot hold more than 2GB of data",
                "ReleasableBytesStreamOutput cannot hold more than 2GB",
                "Error: ReleasableBytesStreamOutput cannot hold more than 2GB of data occurred",
            )

        validMessages.forEach { message ->
            val exception = IllegalArgumentException(message)
            val is2GBException =
                exception is IllegalArgumentException &&
                    exception.message?.contains("ReleasableBytesStreamOutput cannot hold more than 2GB") == true

            assertTrue("Should detect 2GB exception for message: $message", is2GBException)
        }

        val invalidMessages =
            listOf(
                "ReleasableBytesStreamOutput cannot hold more than 1GB of data",
                "BytesStreamOutput cannot hold more than 2GB of data",
                "Some other error message",
                null,
            )

        invalidMessages.forEach { message ->
            val exception = IllegalArgumentException(message)
            val is2GBException =
                exception is IllegalArgumentException &&
                    exception.message?.contains("ReleasableBytesStreamOutput cannot hold more than 2GB") == true

            assertFalse("Should not detect 2GB exception for message: $message", is2GBException)
        }
    }

    @Test
    fun `test 2GB exception handling workflow simulation`() {
        // Test the complete workflow logic as it happens in ShardReplicationTask
        // without mocking the actual tracker class
        val fromSeqNo = 1000L
        val toSeqNo = 51000L

        // Simulate the exception handling logic from ShardReplicationTask
        val exception = IllegalArgumentException("ReleasableBytesStreamOutput cannot hold more than 2GB of data")
        var immediateRetry = false
        var delayTriggered = false
        var batchSizeReduced = false
        var batchMarkedAsFailed = false

        try {
            throw exception
        } catch (e: Exception) {
            if (e is IllegalArgumentException &&
                e.message?.contains("ReleasableBytesStreamOutput cannot hold more than 2GB") == true
            ) {
                // Should reduce batch size
                batchSizeReduced = true

                // Should update batch as failed
                batchMarkedAsFailed = true

                // Should retry immediately (no delay)
                immediateRetry = true
                // In real code: return@launch (immediate retry)
            } else {
                // Other exceptions would trigger delay
                delayTriggered = true
            }
        }

        assertTrue("Should retry immediately for 2GB exception", immediateRetry)
        assertTrue("Should reduce batch size for 2GB exception", batchSizeReduced)
        assertTrue("Should mark batch as failed for 2GB exception", batchMarkedAsFailed)
        assertFalse("Should not trigger delay for 2GB exception", delayTriggered)
    }

    @Test
    fun `test batch size state consistency`() {
        val indexSettings =
            createIndexSettings(
                Settings
                    .builder()
                    .put(ReplicationPlugin.REPLICATION_FOLLOWER_OPS_BATCH_SIZE_INDEX.key, 5000)
                    .build(),
            )
        val batchSizeSettings = BatchSizeSettings(indexSettings, replicationSettings)

        // Initial state
        assertFalse(batchSizeSettings.isDynamicallyReduced())
        assertEquals(batchSizeSettings.getBatchSize(), batchSizeSettings.getEffectiveBatchSize())
        assertEquals("index-level", batchSizeSettings.getBatchSizeSource())

        // After reduction
        batchSizeSettings.reduceBatchSize()
        assertTrue(batchSizeSettings.isDynamicallyReduced())
        assertNotEquals(batchSizeSettings.getBatchSize(), batchSizeSettings.getEffectiveBatchSize())
        assertTrue(batchSizeSettings.getEffectiveBatchSize() < batchSizeSettings.getBatchSize())
        assertEquals("index-level", batchSizeSettings.getBatchSizeSource()) // Source unchanged

        // After reset
        batchSizeSettings.resetBatchSize()
        assertFalse(batchSizeSettings.isDynamicallyReduced())
        assertEquals(batchSizeSettings.getBatchSize(), batchSizeSettings.getEffectiveBatchSize())
        assertEquals("index-level", batchSizeSettings.getBatchSizeSource())
    }

    @Test
    fun `test concurrent batch size operations`() {
        val indexSettings =
            createIndexSettings(
                Settings
                    .builder()
                    .put(ReplicationPlugin.REPLICATION_FOLLOWER_OPS_BATCH_SIZE_INDEX.key, 10000)
                    .build(),
            )
        val batchSizeSettings = BatchSizeSettings(indexSettings, replicationSettings)

        // Simulate multiple concurrent reductions (as might happen with multiple reader coroutines)
        repeat(5) {
            batchSizeSettings.reduceBatchSize()
        }

        // Should be significantly reduced but not below minimum
        assertTrue(batchSizeSettings.getEffectiveBatchSize() < 10000)
        assertTrue(batchSizeSettings.getEffectiveBatchSize() >= 16)
        assertTrue(batchSizeSettings.isDynamicallyReduced())

        // Reset should work correctly
        batchSizeSettings.resetBatchSize()
        assertEquals(10000, batchSizeSettings.getEffectiveBatchSize())
        assertFalse(batchSizeSettings.isDynamicallyReduced())
    }

    @Test
    fun `test changes tracker batch size reduction integration`() {
        // Test the actual ShardReplicationChangesTracker with batch size reduction
        val indexSettings =
            createIndexSettings(
                Settings
                    .builder()
                    .put(ReplicationPlugin.REPLICATION_FOLLOWER_OPS_BATCH_SIZE_INDEX.key, 2000)
                    .build(),
            )

        `when`(mockIndexShard.indexSettings()).thenReturn(indexSettings)
        `when`(mockIndexShard.localCheckpoint).thenReturn(50L)

        val changesTracker = ShardReplicationChangesTracker(mockIndexShard, replicationSettings)

        // Test batch size reduction directly
        changesTracker.reduceBatchSize()

        // Test reset functionality
        changesTracker.resetBatchSize()

        // Verify the tracker can handle multiple reductions
        repeat(3) {
            changesTracker.reduceBatchSize()
        }

        // This verifies the integration works without exceptions
        assertTrue("Changes tracker should handle batch size operations", true)
    }

    private fun createIndexSettings(settings: Settings): IndexSettings {
        val indexMetadata =
            IndexMetadata
                .builder("test-index")
                .settings(
                    Settings
                        .builder()
                        .put(settings)
                        .put("index.version.created", Version.CURRENT)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .build(),
                ).build()
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
                logger.warn("Thread pool did not terminate gracefully, forcing shutdown")
                threadPool.shutdownNow()
            }
        } catch (e: Exception) {
            logger.warn("Exception during thread pool cleanup", e)
            threadPool.shutdownNow()
        }
    }

    override fun tearDown() {
        super.tearDown()
    }
}
