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
import org.opensearch.replication.util.performOp
import org.apache.logging.log4j.LogManager
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.single.shard.TransportSingleShardAction
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.routing.ShardsIterator
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.bytes.BytesArray
import org.opensearch.common.inject.Inject
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.index.shard.ShardId
import org.opensearch.indices.IndicesService
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportActionProxy
import org.opensearch.transport.TransportService

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
