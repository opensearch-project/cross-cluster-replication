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

package org.opensearch.replication.action.stats

import org.opensearch.action.support.nodes.BaseNodeResponse
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.index.shard.ShardId
import org.opensearch.replication.task.shard.FollowerShardMetric
import org.opensearch.replication.task.shard.FollowerShardMetric.FollowerStats
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
