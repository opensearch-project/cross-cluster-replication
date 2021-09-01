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
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent.EMPTY_PARAMS
import org.opensearch.common.xcontent.ToXContent.Params
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.replication.seqno.RemoteShardMetric.RemoteShardStats
import java.io.IOException

class LeaderStatsResponse : BaseNodesResponse<LeaderNodeStatsResponse?>, ToXContentObject {
    var remoteStats :MutableMap<String, RemoteShardStats> = mutableMapOf()
    var ops :Long = 0
    var tlogSize :Long = 0
    var opsLucene :Long = 0
    var opsTlog :Long = 0
    var latencyLucene :Long = 0
    var latencyTlog :Long = 0
    var bytesRead :Long = 0

    companion object {
        private val log = LogManager.getLogger(LeaderStatsResponse::class.java)
    }

    constructor(inp: StreamInput) : super(inp) {
        remoteStats = inp.readMap(StreamInput::readString, ::RemoteShardStats)
    }
    
    fun add(stat :RemoteShardStats) {
        ops += stat.ops
        tlogSize += stat.tlogSize
        opsLucene += stat.opsLucene
        opsTlog += stat.opsTlog
        latencyLucene += stat.latencyLucene
        latencyTlog += stat.latencyTlog
        bytesRead += stat.bytesRead
    }

    constructor(clusterName: ClusterName?, leaderNodeRespons: List<LeaderNodeStatsResponse>?, failures: List<FailedNodeException?>?) : super(clusterName, leaderNodeRespons, failures) {
        if (leaderNodeRespons != null) {
            for (response in leaderNodeRespons) {
                for (entry in response.remoteStats) {
                    remoteStats[entry.key.indexName] = remoteStats.getOrDefault(entry.key.indexName, RemoteShardStats())
                    remoteStats[entry.key.indexName]!!.add(entry.value)
                    add(entry.value)
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
        builder.field("operations_read", ops)
        builder.field("translog_size_bytes", tlogSize)
        builder.field("operations_read_lucene", opsLucene)
        builder.field("operations_read_translog", opsTlog)
        builder.field("total_read_time_lucene_millis", latencyLucene)
        builder.field("total_read_time_translog_millis", latencyTlog)
        builder.field("bytes_read", bytesRead)
        builder.field("index_details").map(remoteStats)
        builder.endObject()
        return builder
    }

    override fun toString(): String {
        val builder: XContentBuilder = XContentFactory.jsonBuilder().prettyPrint()
        toXContent(builder, EMPTY_PARAMS)
        return Strings.toString(builder)
    }
}

