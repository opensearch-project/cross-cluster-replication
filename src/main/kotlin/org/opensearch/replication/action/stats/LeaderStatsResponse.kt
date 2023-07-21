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


import org.apache.logging.log4j.LogManager
import org.opensearch.action.FailedNodeException
import org.opensearch.action.support.nodes.BaseNodesResponse
import org.opensearch.cluster.ClusterName
import org.opensearch.common.Strings
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS
import org.opensearch.core.xcontent.ToXContent.Params
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.replication.seqno.RemoteShardMetric
import org.opensearch.replication.seqno.RemoteShardMetric.RemoteStats
import java.io.IOException

class LeaderStatsResponse : BaseNodesResponse<LeaderNodeStatsResponse?>, ToXContentObject {
    var remoteStats :MutableMap<String, RemoteStats> = mutableMapOf()
    var stats = RemoteShardMetric.RemoteStatsFrag()

    companion object {
        private val log = LogManager.getLogger(LeaderStatsResponse::class.java)
    }

    constructor(inp: StreamInput) : super(inp) {
        remoteStats = inp.readMap(StreamInput::readString, ::RemoteStats)
    }


    constructor(clusterName: ClusterName?, leaderNodeRespons: List<LeaderNodeStatsResponse>?, failures: List<FailedNodeException?>?) : super(clusterName, leaderNodeRespons, failures) {
        if (leaderNodeRespons != null) {
            for (response in leaderNodeRespons) {
                for (entry in response.remoteStats) {
                    remoteStats[entry.key.indexName] = remoteStats.getOrDefault(entry.key.indexName, RemoteStats())
                    remoteStats[entry.key.indexName]!!.add(entry.value)
                    stats.add(entry.value)
                }
            }
        }
    }

    @Throws(IOException::class)
    override fun readNodesFrom(inp: StreamInput): List<LeaderNodeStatsResponse> {
        return inp.readList { LeaderNodeStatsResponse(inp) }
        }

    @Throws(IOException::class)
     override fun writeNodesTo(out: StreamOutput, leaderNodeRespons: List<LeaderNodeStatsResponse?>?) {
        out.writeList(leaderNodeRespons)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: Params?): XContentBuilder {
        builder.startObject()
        builder.field("num_replicated_indices", remoteStats.size)
        stats.toXContent(builder, params)
        builder.field("index_stats").map(remoteStats)
        builder.endObject()
        return builder
    }

    override fun toString(): String {
        val builder: XContentBuilder = XContentFactory.jsonBuilder().prettyPrint()
        toXContent(builder, EMPTY_PARAMS)
        return Strings.toString(builder)
    }
}

