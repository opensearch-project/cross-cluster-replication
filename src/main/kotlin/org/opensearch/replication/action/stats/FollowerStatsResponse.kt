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
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.index.shard.ShardId
import org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS
import org.opensearch.core.xcontent.ToXContent.Params
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.replication.metadata.ReplicationOverallState
import org.opensearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import org.opensearch.replication.metadata.state.ReplicationStateMetadata
import org.opensearch.replication.task.shard.FollowerShardMetric
import org.opensearch.replication.task.shard.FollowerShardMetric.FollowerStats
import java.io.IOException

class FollowerStatsResponse :
    BaseNodesResponse<FollowerNodeStatsResponse?>,
    ToXContentObject {
    var shardStats: MutableMap<ShardId, FollowerStats> = mutableMapOf()
    var indexStats: MutableMap<String, FollowerStats> = mutableMapOf()
    var stats: FollowerShardMetric.FollowerStatsFragment = FollowerShardMetric.FollowerStatsFragment()

    var pausedIndices: Int = 0
    var failedIndices: Int = 0
    var bootstrappingIndices: Int = 0
    var syncingIndices: Int = 0
    var shardTaskCount: Int = 0
    var indexTaskCount: Int = 0

    companion object {
        private val log = LogManager.getLogger(FollowerStatsResponse::class.java)
    }

    constructor(inp: StreamInput) : super(inp) {
        shardStats = inp.readMap(::ShardId, ::FollowerStats)
    }

    constructor(
        clusterName: ClusterName?,
        followerNodeResponse: List<FollowerNodeStatsResponse>?,
        failures: List<FailedNodeException?>?,
        metadata: ReplicationStateMetadata,
    ) : super(
        clusterName,
        followerNodeResponse,
        failures,
    ) {

        var syncing: MutableSet<String> = mutableSetOf()
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

        var totalRunning = 0 // includes boostrap and syncing
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
    override fun readNodesFrom(inp: StreamInput): List<FollowerNodeStatsResponse> = inp.readList { FollowerNodeStatsResponse(inp) }

    @Throws(IOException::class)
    override fun writeNodesTo(
        out: StreamOutput,
        leaderNodeRespons: List<FollowerNodeStatsResponse?>?,
    ) {
        out.writeList(leaderNodeRespons)
    }

    @Throws(IOException::class)
    override fun toXContent(
        builder: XContentBuilder,
        params: Params?,
    ): XContentBuilder {
        builder.startObject()
        builder.field("num_syncing_indices", syncingIndices)
        builder.field("num_bootstrapping_indices", bootstrappingIndices)
        builder.field("num_paused_indices", pausedIndices)
        builder.field("num_failed_indices", failedIndices)
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
        return builder.toString()
    }
}
