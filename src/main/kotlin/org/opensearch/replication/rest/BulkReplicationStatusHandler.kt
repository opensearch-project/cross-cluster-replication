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

package org.opensearch.replication.rest

import org.opensearch.action.support.IndicesOptions
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.replication.action.status.ReplicationStatusAction
import org.opensearch.replication.action.status.ReplicationStatusResponse
import org.opensearch.replication.action.status.ShardInfoRequest
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.transport.client.node.NodeClient
import org.apache.logging.log4j.LogManager
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

class BulkReplicationStatusHandler @Inject constructor(
    private val clusterService: ClusterService,
    private val indexNameExpressionResolver: IndexNameExpressionResolver
) : BaseRestHandler() {

    companion object {
        private val log = LogManager.getLogger(BulkReplicationStatusHandler::class.java)
    }

    override fun routes() = listOf(RestHandler.Route(RestRequest.Method.GET, "/_plugins/_replication/_bulk_status"))
    override fun getName() = "plugins_index_bulk_replication_status"

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val pattern = request.param("pattern")
            ?: return RestChannelConsumer { channel ->
                channel.sendResponse(BytesRestResponse(RestStatus.BAD_REQUEST, "pattern parameter is required"))
            }
        val pretty = request.paramAsBoolean("pretty", false)

        return RestChannelConsumer { channel ->
            val state = clusterService.state()
            val indices = indexNameExpressionResolver.concreteIndexNames(
                state, IndicesOptions.lenientExpandOpen(), pattern
            ).sortedWith(compareBy<String> { it.split("-").last().toIntOrNull() ?: Int.MAX_VALUE }.thenBy { it })

            if (indices.isEmpty()) {
                val builder = XContentFactory.jsonBuilder().also { if (pretty) it.prettyPrint() }
                builder.startObject().startObject("indices").endObject().endObject()
                channel.sendResponse(BytesRestResponse(RestStatus.OK, builder))
                return@RestChannelConsumer
            }

            val results = ConcurrentHashMap<String, ReplicationStatusResponse>()
            val remaining = AtomicInteger(indices.size)

            fun tryRespond() {
                if (remaining.decrementAndGet() == 0) {
                    val builder = XContentFactory.jsonBuilder().also { if (pretty) it.prettyPrint() }
                    builder.startObject()
                    builder.startObject("indices")
                    indices.forEach { index ->
                        builder.startObject(index)
                        val status = results[index]
                        if (status != null) {
                            builder.field("status", status.status)
                            try { if (status.reason.isNotEmpty()) builder.field("reason", status.reason) } catch (_: Exception) {}
                            try { if (status.connectionAlias.isNotEmpty()) builder.field("leader_alias", status.connectionAlias) } catch (_: Exception) {}
                            try { if (status.leaderIndexName.isNotEmpty()) builder.field("leader_index", status.leaderIndexName) } catch (_: Exception) {}
                            try { if (status.followerIndexName.isNotEmpty()) builder.field("follower_index", status.followerIndexName) } catch (_: Exception) {}
                            try { builder.field("syncing_details", status.aggregatedReplayDetails) } catch (_: Exception) {}
                            try { builder.field("bootstrap_details", status.aggregatedRestoreDetails) } catch (_: Exception) {}
                        }
                        builder.endObject()
                    }
                    builder.endObject()
                    builder.endObject()
                    channel.sendResponse(BytesRestResponse(RestStatus.OK, builder))
                }
            }

            indices.forEach { index ->
                client.execute(ReplicationStatusAction.INSTANCE, ShardInfoRequest(index, false),
                    object : ActionListener<ReplicationStatusResponse> {
                        override fun onResponse(response: ReplicationStatusResponse) {
                            results[index] = response
                            tryRespond()
                        }
                        override fun onFailure(e: Exception) {
                            results[index] = ReplicationStatusResponse(e.message ?: "error fetching status")
                            tryRespond()
                        }
                    })
            }
        }
    }
}
