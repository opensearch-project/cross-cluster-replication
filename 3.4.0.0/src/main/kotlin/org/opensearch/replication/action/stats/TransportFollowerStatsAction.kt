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
import org.opensearch.replication.metadata.state.ReplicationStateMetadata
import org.opensearch.replication.seqno.RemoteClusterStats
import org.opensearch.replication.task.shard.FollowerClusterStats
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService

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
