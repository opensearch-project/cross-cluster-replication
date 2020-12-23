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
import com.amazon.elasticsearch.replication.util.performOp
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.routing.ShardsIterator
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.bytes.BytesArray
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.indices.IndicesService
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportActionProxy
import org.elasticsearch.transport.TransportService

class TransportGetFileChunkAction @Inject constructor(threadPool: ThreadPool, clusterService: ClusterService,
                                                      transportService: TransportService, actionFilters: ActionFilters,
                                                      indexNameExpressionResolver: IndexNameExpressionResolver,
                                                      private val indicesService: IndicesService,
                                                      private val restoreLeaderService: RemoteClusterRestoreLeaderService) :
        TransportSingleShardAction<GetFileChunkRequest, GetFileChunkResponse>(GetFileChunkAction.NAME,
                threadPool, clusterService, transportService, actionFilters,
                indexNameExpressionResolver, ::GetFileChunkRequest, ThreadPool.Names.GET) {

    init {
        TransportActionProxy.registerProxyAction(transportService, GetFileChunkAction.NAME, ::GetFileChunkResponse)
    }

    companion object {
        private val log = LogManager.getLogger(TransportGetFileChunkAction::class.java)
    }

    override fun shardOperation(request: GetFileChunkRequest, shardId: ShardId): GetFileChunkResponse {
        log.debug(request.toString())
        val indexShard = indicesService.indexServiceSafe(shardId.index).getShard(shardId.id)
        val store = indexShard.store()
        val buffer = ByteArray(request.length)
        var bytesRead = 0

        store.performOp({
            val fileMetaData = request.storeFileMetadata
            val currentInput = restoreLeaderService.openInputStream(request.restoreUUID, request,
                    fileMetaData.name(), fileMetaData.length())
            val offset = request.offset
            if (offset < fileMetaData.length()) {
                currentInput.skip(offset)
                bytesRead = currentInput.read(buffer)
            }
        })

        return GetFileChunkResponse(request.storeFileMetadata, request.offset, BytesArray(buffer, 0, bytesRead))
    }

    override fun resolveIndex(request: GetFileChunkRequest): Boolean {
        return true
    }

    override fun getResponseReader(): Writeable.Reader<GetFileChunkResponse> {
        return Writeable.Reader { inp: StreamInput -> GetFileChunkResponse(inp) }
    }

    override fun shards(state: ClusterState, request: InternalRequest): ShardsIterator? {
        return state.routingTable().shardRoutingTable(request.request().leaderShardId).primaryShardIt()
    }

}
