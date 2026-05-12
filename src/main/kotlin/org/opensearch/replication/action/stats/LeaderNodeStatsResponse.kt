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
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.index.shard.ShardId
import org.opensearch.replication.seqno.RemoteShardMetric
import org.opensearch.replication.seqno.RemoteShardMetric.RemoteStats
import java.io.IOException

class LeaderNodeStatsResponse : BaseNodeResponse {
    var remoteStats :Map<ShardId, RemoteStats>

    constructor(inp: StreamInput) : super(inp) {
        remoteStats = inp.readMap(::ShardId, ::RemoteStats)
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
