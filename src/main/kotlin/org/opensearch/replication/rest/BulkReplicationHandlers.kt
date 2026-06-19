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

import org.opensearch.action.ActionType
import org.opensearch.replication.action.bulk.BulkPauseReplicationAction
import org.opensearch.replication.action.bulk.BulkReplicationRequest
import org.opensearch.replication.action.bulk.BulkReplicationResponse
import org.opensearch.replication.action.bulk.BulkResumeReplicationAction
import org.opensearch.replication.action.bulk.BulkStartReplicationAction
import org.opensearch.replication.action.bulk.BulkStopReplicationAction
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.transport.client.node.NodeClient

private class BulkReplicationHandler(
    private val method: RestRequest.Method,
    private val route: String,
    private val handlerName: String,
    private val action: ActionType<BulkReplicationResponse>
) : BaseRestHandler() {
    override fun routes() = listOf(RestHandler.Route(method, route))
    override fun getName() = handlerName
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val req = request.contentOrSourceParamParser().use { BulkReplicationRequest.fromXContent(it) }
        return RestChannelConsumer { channel: RestChannel ->
            client.execute(action, req, RestToXContentListener<BulkReplicationResponse>(channel))
        }
    }
}

fun bulkReplicationHandlers(): List<BaseRestHandler> = listOf(
    BulkReplicationHandler(RestRequest.Method.POST, "/_plugins/_replication/_bulk_start",
        "plugins_index_bulk_start_replication_action", BulkStartReplicationAction.INSTANCE),
    BulkReplicationHandler(RestRequest.Method.POST, "/_plugins/_replication/_bulk_stop",
        "plugins_index_bulk_stop_replication_action", BulkStopReplicationAction.INSTANCE),
    BulkReplicationHandler(RestRequest.Method.POST, "/_plugins/_replication/_bulk_pause",
        "plugins_index_bulk_pause_replication_action", BulkPauseReplicationAction.INSTANCE),
    BulkReplicationHandler(RestRequest.Method.POST, "/_plugins/_replication/_bulk_resume",
        "plugins_index_bulk_resume_replication_action", BulkResumeReplicationAction.INSTANCE)
)
