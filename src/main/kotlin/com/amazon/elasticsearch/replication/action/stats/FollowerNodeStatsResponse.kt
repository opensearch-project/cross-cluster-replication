/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The elasticsearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright elasticsearch Contributors. See
 * GitHub history for details.
 */

package org.elasticsearch.replication.action.stats

import com.amazon.elasticsearch.replication.task.shard.FollowerShardMetric
import com.amazon.elasticsearch.replication.task.shard.FollowerShardMetric.FollowerStats
import org.elasticsearch.action.support.nodes.BaseNodeResponse
import org.elasticsearch.cluster.node.DiscoveryNode
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.index.shard.ShardId
import java.io.IOException

class FollowerNodeStatsResponse : BaseNodeResponse {
    var stats :Map<ShardId, FollowerStats>

    constructor(inp: StreamInput) : super(inp) {
        stats = inp.readMap(::ShardId, ::FollowerStats)
    }

    constructor(node : DiscoveryNode, remoteClusterStats: Map<ShardId, FollowerShardMetric>) : super(node) {
        stats = remoteClusterStats.mapValues { (_ , v) -> v.createStats()  }
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeMap(stats, { o, k -> k.writeTo(o)}, { o, v -> v.writeTo(o)})
    }
}
