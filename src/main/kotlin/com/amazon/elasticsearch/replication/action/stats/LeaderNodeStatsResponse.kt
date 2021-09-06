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

package com.amazon.elasticsearch.replication.action.stats

import com.amazon.elasticsearch.replication.seqno.RemoteShardMetric
import com.amazon.elasticsearch.replication.seqno.RemoteShardMetric.RemoteShardStats
import org.elasticsearch.action.support.nodes.BaseNodeResponse
import org.elasticsearch.cluster.node.DiscoveryNode
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.index.shard.ShardId
import java.io.IOException

class LeaderNodeStatsResponse : BaseNodeResponse {
    var remoteStats :Map<ShardId, RemoteShardMetric.RemoteShardStats>

    constructor(inp: StreamInput) : super(inp) {
        remoteStats = inp.readMap(::ShardId, ::RemoteShardStats)
    }

    constructor(node : DiscoveryNode, remoteClusterStats: Map<ShardId, RemoteShardMetric>) : super(node) {
        remoteStats = remoteClusterStats.mapValues { (_ , v) -> v.createStats()  }
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeMap(remoteStats, { o, k -> k.writeTo(o)}, { o, v -> v.writeTo(o)})
    }
}
