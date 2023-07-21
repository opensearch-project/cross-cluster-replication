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

class TransportGetStoreMetadataAction @Inject constructor(threadPool: ThreadPool, clusterService: ClusterService,
                                                          transportService: TransportService, actionFilters: ActionFilters,
                                                          indexNameExpressionResolver: IndexNameExpressionResolver,
                                                          private val restoreLeaderService: RemoteClusterRestoreLeaderService) :
        TransportSingleShardAction<GetStoreMetadataRequest, GetStoreMetadataResponse>(GetStoreMetadataAction.NAME,
                threadPool, clusterService, transportService, actionFilters,
                indexNameExpressionResolver, ::GetStoreMetadataRequest, ThreadPool.Names.GET) {
    init {
        TransportActionProxy.registerProxyAction(transportService, GetStoreMetadataAction.NAME, ::GetStoreMetadataResponse)
    }

    companion object {
        private val log = LogManager.getLogger(TransportGetStoreMetadataAction::class.java)
    }

    override fun shardOperation(request: GetStoreMetadataRequest, shardId: ShardId): GetStoreMetadataResponse {
        log.debug(request.toString())
        var metadataSnapshot = restoreLeaderService.addLeaderClusterRestore(request.restoreUUID, request).metadataSnapshot
        return GetStoreMetadataResponse(metadataSnapshot)
    }

    override fun resolveIndex(request: GetStoreMetadataRequest): Boolean {
        return true
    }

    override fun getResponseReader(): Writeable.Reader<GetStoreMetadataResponse> {
        return Writeable.Reader { inp: StreamInput -> GetStoreMetadataResponse(inp) }
    }

    override fun shards(state: ClusterState, request: InternalRequest): ShardsIterator {
        return state.routingTable().shardRoutingTable(request.request().leaderShardId).primaryShardIt()
    }
}
