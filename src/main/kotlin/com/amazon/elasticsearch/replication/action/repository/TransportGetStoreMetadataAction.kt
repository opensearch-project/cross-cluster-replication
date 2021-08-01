/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.elasticsearch.replication.action.repository

import com.amazon.elasticsearch.replication.repository.RemoteClusterRestoreLeaderService
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.routing.ShardsIterator
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportActionProxy
import org.elasticsearch.transport.TransportService

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
