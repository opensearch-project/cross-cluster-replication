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

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import org.apache.logging.log4j.LogManager
import org.opensearch.action.FailedNodeException
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.nodes.TransportNodesAction
import org.opensearch.transport.client.node.NodeClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.index.shard.ShardId
import org.opensearch.indices.IndicesService
import org.opensearch.replication.seqno.RemoteClusterRetentionLeaseHelper.Companion.RETENTION_LEASE_PREFIX
import org.opensearch.replication.seqno.RemoteClusterStats
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import java.util.concurrent.TimeUnit

class TransportLeaderStatsAction @Inject constructor(transportService: TransportService,
                                                     clusterService: ClusterService,
                                                     threadPool: ThreadPool,
                                                     actionFilters: ActionFilters,
                                                     private val remoteStats: RemoteClusterStats,
                                                     private val indicesService: IndicesService,
                                                     private val client: NodeClient) :
        TransportNodesAction<LeaderStatsRequest, LeaderStatsResponse, NodeStatsRequest, LeaderNodeStatsResponse>(LeaderStatsAction.NAME,
             threadPool, clusterService, transportService,  actionFilters, ::LeaderStatsRequest,  ::NodeStatsRequest, ThreadPool.Names.MANAGEMENT,
                LeaderNodeStatsResponse::class.java), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportLeaderStatsAction::class.java)
        val durationThresholdActiveReplication =TimeUnit.SECONDS.toMillis(300) //5 min
    }

    override fun newNodeRequest(request: LeaderStatsRequest): NodeStatsRequest {
       return NodeStatsRequest()
    }

    override fun newNodeResponse(input: StreamInput): LeaderNodeStatsResponse {
        return LeaderNodeStatsResponse(input)
    }

    override fun newResponse(request: LeaderStatsRequest?, responses: MutableList<LeaderNodeStatsResponse>?, failures: MutableList<FailedNodeException>?): LeaderStatsResponse {
        return LeaderStatsResponse(clusterService.clusterName, responses, failures)

    }

     private fun isShardReplicationActive(shardId :ShardId) :Boolean {
        val indicesRouting = clusterService.state().routingTable.indicesRouting
         if (!indicesRouting.containsKey(shardId.indexName)) {
             return false
         }

         val indexService = indicesService.indexService(shardId.index) ?: return false
         val indexShard = indexService.getShard(shardId.id) ?: return false

         val retentionLeases = indexShard.getRetentionLeases().leases()
         for (retentionLease in retentionLeases) {
             if (retentionLease.id().startsWith(RETENTION_LEASE_PREFIX)) {
                 return true
             }
         }

         return false
    }

    override fun nodeOperation(nodeStatRequest: NodeStatsRequest?): LeaderNodeStatsResponse {
        var activeStats = remoteStats.stats.filter { (k, v) ->
            var indexExists = isShardReplicationActive(k)
            val timeSinceLastFetch = TimeUnit.NANOSECONDS.toMillis( System.nanoTime() - v.lastFetchTime.get() )
            (timeSinceLastFetch < durationThresholdActiveReplication ) && indexExists
        }

       return LeaderNodeStatsResponse(this.clusterService.localNode(), activeStats)
    }
}

