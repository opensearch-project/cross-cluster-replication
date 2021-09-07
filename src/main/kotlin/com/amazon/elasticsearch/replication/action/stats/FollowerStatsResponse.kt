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


import com.amazon.elasticsearch.replication.metadata.ReplicationOverallState
import com.amazon.elasticsearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import com.amazon.elasticsearch.replication.metadata.state.ReplicationStateMetadata
import com.amazon.elasticsearch.replication.task.shard.FollowerShardMetric
import com.amazon.elasticsearch.replication.task.shard.FollowerShardMetric.FollowerStats
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
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.replication.action.stats.FollowerNodeStatsResponse
import java.io.IOException

class FollowerStatsResponse : BaseNodesResponse<FollowerNodeStatsResponse?>, ToXContentObject {
    var shardStats :MutableMap<ShardId, FollowerShardMetric.FollowerStats> = mutableMapOf()
    var indexStats :MutableMap<String, FollowerShardMetric.FollowerStats> = mutableMapOf()
    var stats : FollowerShardMetric.FollowerStatsFragment = FollowerShardMetric.FollowerStatsFragment()

    var pausedIndices :Int = 0
    var failedIndices :Int = 0
    var bootstrappingIndices :Int = 0
    var syncingIndices :Int = 0
    var shardTaskCount :Int = 0
    var indexTaskCount :Int = 0

    companion object {
        private val log = LogManager.getLogger(FollowerStatsResponse::class.java)
    }

    constructor(inp: StreamInput) : super(inp) {
        shardStats = inp.readMap(::ShardId, ::FollowerStats)
    }

    constructor(clusterName: ClusterName?, followerNodeResponse: List<FollowerNodeStatsResponse>?, failures: List<FailedNodeException?>?
                , metadata : ReplicationStateMetadata) : super(clusterName, followerNodeResponse, failures) {

        var syncing :MutableSet<String> = mutableSetOf()
        if (followerNodeResponse != null) {
            for (response in followerNodeResponse) {
                shardStats.putAll(response.stats)

                for (i in response.stats) {
                    syncing.add(i.key.indexName)

                    if (i.key.indexName !in indexStats) {
                        indexStats[i.key.indexName] = FollowerShardMetric.FollowerStats()
                    }
                    indexStats[i.key.indexName]!!.add(i.value)

                    stats.add(i.value)
                }
            }
        }

        var totalRunning = 0 //includes boostrap and syncing
        for (entry in metadata.replicationDetails) {
            when (entry.value[REPLICATION_LAST_KNOWN_OVERALL_STATE]) {
                ReplicationOverallState.RUNNING.name -> totalRunning++
                ReplicationOverallState.FAILED.name -> failedIndices++
                ReplicationOverallState.PAUSED.name -> pausedIndices++
            }
        }

        syncingIndices = syncing.size
        bootstrappingIndices = totalRunning - syncingIndices

        shardTaskCount = shardStats.size
        indexTaskCount = totalRunning
    }

    @Throws(IOException::class)
    override fun readNodesFrom(inp: StreamInput): List<FollowerNodeStatsResponse> {
        return inp.readList { FollowerNodeStatsResponse(inp) }
    }

    @Throws(IOException::class)
     override fun writeNodesTo(out: StreamOutput, leaderNodeRespons: List<FollowerNodeStatsResponse?>?) {
        out.writeList(leaderNodeRespons)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: Params?): XContentBuilder {
        builder.startObject()
        builder.field("num_syncing_indices", syncingIndices)
        builder.field("num_bootstrapping_indices", bootstrappingIndices)
        builder.field("num_paused_indices", pausedIndices)
        builder.field("num_shard_tasks", shardTaskCount)
        builder.field("num_index_tasks", indexTaskCount)
        stats.toXContent(builder, params)
        builder.field("index_stats").map(indexStats)
        builder.endObject()
        return builder
    }

    override fun toString(): String {
        val builder: XContentBuilder = XContentFactory.jsonBuilder().prettyPrint()
        toXContent(builder, EMPTY_PARAMS)
        return Strings.toString(builder)
    }
}

