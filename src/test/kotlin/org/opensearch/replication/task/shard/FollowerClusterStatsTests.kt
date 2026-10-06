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

import org.opensearch.core.index.Index
import org.opensearch.core.index.shard.ShardId
import org.opensearch.test.OpenSearchTestCase
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

class FollowerClusterStatsTests : OpenSearchTestCase() {

    private val shardId = ShardId(Index("follower-index", "_na_"), 0)

    fun `test refreshFollowerCheckpoint advances a stale checkpoint to the live value`() {
        val stats = FollowerClusterStats()
        stats.stats[shardId] = FollowerShardMetric()
        // Value left behind by the last successful write, before the leader went idle
        // and subsequent getChanges calls started timing out with nothing to replay.
        stats.stats[shardId]!!.followerCheckpoint = 218196L

        stats.refreshFollowerCheckpoint(shardId, 218323L)

        assertEquals(218323L, stats.stats[shardId]!!.followerCheckpoint)
    }

    fun `test refreshFollowerCheckpoint is a no-op when the shard task is not tracked`() {
        val stats = FollowerClusterStats()

        stats.refreshFollowerCheckpoint(shardId, 100L)

        assertFalse(stats.stats.containsKey(shardId))
    }

    // add imports:
    // import java.util.concurrent.ConcurrentHashMap
    // import java.util.concurrent.atomic.AtomicBoolean
    // import java.util.concurrent.atomic.AtomicLong

    /**
     * Reproduces the 2026-08-27 production NPE storm. FollowerClusterStats.stats is a plain
     * mutableMapOf() (LinkedHashMap), a node-wide @Singleton, concurrently read by every shard
     * reader (stats[followerShardId]!! in the ShardReplicationTask getChanges error path) and
     * mutated by task start/cleanup. A leader blue/green produces a burst of GetChanges failures
     * (reads) WHILE many shard tasks start/stop (put/remove) -- concurrent structural modification
     * of a non-thread-safe map, which can return null for a PRESENT key mid-resize; the unguarded
     * !! then throws NPE (fired ~738x across 10 nodes).
     */
    fun `test FollowerClusterStats is thread-safe under concurrent churn (regression for the Aug-27 NPE)`() {
        val stats = FollowerClusterStats()
        val hot = ShardId(Index("follower-index", "_na_"), 0)
        stats.stats[hot] = FollowerShardMetric()                       // always present; never removed
        val npe = runStressHarness(
            read = { stats.stats[hot]!!.opsReadFailures.addAndGet(1) },
            put = { k -> stats.stats[k] = FollowerShardMetric() },
            remove = { k -> stats.stats.remove(k) })
        assertEquals("FollowerClusterStats (ConcurrentHashMap) must never NPE under concurrent churn", 0L, npe)
    }

    /** Validates the fix: ConcurrentHashMap survives the identical churn with zero NPEs. */
    fun `test ConcurrentHashMap survives the same concurrent churn`() {
        val stats: MutableMap<ShardId, FollowerShardMetric> = ConcurrentHashMap()
        val hot = ShardId(Index("follower-index", "_na_"), 0)
        stats[hot] = FollowerShardMetric()
        val npe = runStressHarness(
            read = { stats[hot]!!.opsReadFailures.addAndGet(1) },
            put = { k -> stats[k] = FollowerShardMetric() },
            remove = { k -> stats.remove(k) })
        assertEquals("ConcurrentHashMap must not NPE under concurrent churn", 0L, npe)
    }

    private fun runStressHarness(
        read: () -> Unit, put: (ShardId) -> Unit, remove: (ShardId) -> Unit,
        writers: Int = 6, readers: Int = 6, budgetMillis: Long = 5000): Long {
        val stop = AtomicBoolean(false); val npe = AtomicLong(0); val threads = mutableListOf<Thread>()
        repeat(writers) { w ->
            threads += Thread {
                var i = 0
                while (!stop.get()) {
                    try { val k = ShardId(Index("idx-$w-${i and 0x3FFF}", "_na_"), 0); put(k); remove(k); i++ }
                    catch (t: Throwable) { }                           // writer-side CME is also a symptom
                }
            }
        }
        repeat(readers) {
            threads += Thread {
                while (!stop.get()) {
                    try { read() } catch (e: NullPointerException) { npe.incrementAndGet() } catch (t: Throwable) { }
                }
            }
        }
        threads.forEach { it.start() }
        val deadline = System.currentTimeMillis() + budgetMillis
        while (System.currentTimeMillis() < deadline && npe.get() == 0L) Thread.sleep(10)
        stop.set(true); threads.forEach { it.join(2000) }
        return npe.get()
    }

    fun `test stat update for an absent shard entry is a no-op`() {
        val stats = FollowerClusterStats()
        val absent = ShardId(Index("missing", "_na_"), 0)
        stats.stats[absent]?.opsReadFailures?.addAndGet(1)
        stats.stats[absent]?.followerCheckpoint = 42L
        assertFalse(stats.stats.containsKey(absent))
    }

    fun `test concurrent remove of the read key never NPEs`() {
        val stats = FollowerClusterStats()
        val key = ShardId(Index("follower-index", "_na_"), 0)
        stats.stats[key] = FollowerShardMetric()
        val npe = runStressHarness(
            read = { stats.stats[key]?.opsReadFailures?.addAndGet(1) },
            put = { _ -> stats.stats[key] = FollowerShardMetric() },
            remove = { _ -> stats.stats.remove(key) })
        assertEquals("guarded read must not NPE when the read key is concurrently removed", 0L, npe)
    }
}
