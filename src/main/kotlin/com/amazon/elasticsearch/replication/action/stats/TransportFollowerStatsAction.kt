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

package com.amazon.elasticsearch.replication.action.stats

import com.amazon.elasticsearch.replication.metadata.state.ReplicationStateMetadata
import com.amazon.elasticsearch.replication.seqno.RemoteClusterStats
import com.amazon.elasticsearch.replication.task.shard.FollowerClusterStats
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.FailedNodeException
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.nodes.TransportNodesAction
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.replication.action.stats.FollowerNodeStatsResponse
import org.elasticsearch.replication.action.stats.FollowerStatsAction
import org.elasticsearch.replication.action.stats.FollowerStatsRequest
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService

class TransportFollowerStatsAction @Inject constructor(transportService: TransportService,
                                                       clusterService: ClusterService,
                                                       threadPool: ThreadPool,
                                                       actionFilters: ActionFilters,
                                                       private val remoteStats: RemoteClusterStats,
                                                       private val client: NodeClient,
                                                       private val followerStats: FollowerClusterStats) :
        TransportNodesAction<FollowerStatsRequest, FollowerStatsResponse, NodeStatsRequest, FollowerNodeStatsResponse>(FollowerStatsAction.NAME,
             threadPool, clusterService, transportService,  actionFilters, ::FollowerStatsRequest,  ::NodeStatsRequest, ThreadPool.Names.MANAGEMENT,
                FollowerNodeStatsResponse::class.java), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportFollowerStatsAction::class.java)
    }

    override fun newNodeRequest(request: FollowerStatsRequest): NodeStatsRequest {
       return NodeStatsRequest()
    }

    override fun newNodeResponse(input: StreamInput): FollowerNodeStatsResponse {
        return FollowerNodeStatsResponse(input)
    }

    override fun newResponse(request: FollowerStatsRequest?, responses: MutableList<FollowerNodeStatsResponse>?, failures: MutableList<FailedNodeException>?): FollowerStatsResponse {
        val metadata = clusterService.state().metadata().custom(ReplicationStateMetadata.NAME) ?: ReplicationStateMetadata.EMPTY
        return FollowerStatsResponse(clusterService.clusterName, responses, failures, metadata)
    }

    override fun nodeOperation(nodeStatRequest: NodeStatsRequest?): FollowerNodeStatsResponse {
       return FollowerNodeStatsResponse(this.clusterService.localNode(), followerStats.stats)
    }
}
