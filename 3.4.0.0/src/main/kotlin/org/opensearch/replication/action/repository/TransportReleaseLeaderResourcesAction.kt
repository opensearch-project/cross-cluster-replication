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

package org.opensearch.replication.action.repository

import org.opensearch.replication.repository.RemoteClusterRestoreLeaderService
import org.apache.logging.log4j.LogManager
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.action.support.single.shard.TransportSingleShardAction
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.routing.ShardsIterator
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.index.shard.ShardId
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportActionProxy
import org.opensearch.transport.TransportService

class TransportReleaseLeaderResourcesAction @Inject constructor(threadPool: ThreadPool, clusterService: ClusterService,
                                                                transportService: TransportService, actionFilters: ActionFilters,
                                                                indexNameExpressionResolver: IndexNameExpressionResolver,
                                                                private val restoreLeaderService: RemoteClusterRestoreLeaderService) :
        TransportSingleShardAction<ReleaseLeaderResourcesRequest, AcknowledgedResponse>(ReleaseLeaderResourcesAction.NAME,
                threadPool, clusterService, transportService, actionFilters,
                indexNameExpressionResolver, ::ReleaseLeaderResourcesRequest, ThreadPool.Names.GET) {
    init {
        TransportActionProxy.registerProxyAction(transportService, ReleaseLeaderResourcesAction.NAME, ::AcknowledgedResponse)
    }

    companion object {
        private val log = LogManager.getLogger(TransportReleaseLeaderResourcesAction::class.java)
    }

    override fun shardOperation(request: ReleaseLeaderResourcesRequest, shardId: ShardId): AcknowledgedResponse {
        log.info("Releasing resources for $shardId with restore-id as ${request.restoreUUID}")
        restoreLeaderService.removeLeaderClusterRestore(request.restoreUUID)
        return AcknowledgedResponse(true)
    }

    override fun resolveIndex(request: ReleaseLeaderResourcesRequest?): Boolean {
        return true
    }

    override fun getResponseReader(): Writeable.Reader<AcknowledgedResponse> {
        return Writeable.Reader { inp: StreamInput -> AcknowledgedResponse(inp) }
    }

    override fun shards(state: ClusterState, request: InternalRequest): ShardsIterator? {
        return state.routingTable().shardRoutingTable(request.request().leaderShardId).primaryShardIt()
    }
}
