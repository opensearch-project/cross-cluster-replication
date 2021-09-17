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
import com.amazon.elasticsearch.replication.seqno.RemoteShardMetric.RemoteStats
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.FailedNodeException
import org.elasticsearch.action.support.nodes.BaseNodesResponse
import org.elasticsearch.cluster.ClusterName
import org.elasticsearch.common.Strings
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS
import org.elasticsearch.common.xcontent.ToXContent.Params
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory
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

