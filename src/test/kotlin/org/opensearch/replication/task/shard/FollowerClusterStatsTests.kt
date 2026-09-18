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
}
