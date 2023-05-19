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

package org.opensearch.replication.rest.bulk

import org.opensearch.replication.action.stop.StopIndexReplicationAction
import org.opensearch.client.node.NodeClient
import org.opensearch.replication.action.bulk.pause.BulkPauseReplicationAction
import org.opensearch.replication.action.bulk.pause.BulkPauseReplicationRequest
import org.opensearch.replication.action.bulk.stop.BulkStopReplicationAction
import org.opensearch.replication.action.bulk.stop.BulkStopReplicationRequest
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

class BulkPauseIndexReplicationHandler : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> {
        return listOf(RestHandler.Route(RestRequest.Method.POST, "/_plugins/_replication/_bulk_pause"))
    }

    override fun getName(): String {
        return "plugins_index_bulk_pause_replicate_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        request.contentOrSourceParamParser().use { parser ->

            val bulkPauseReplicationRequest = BulkPauseReplicationRequest.fromXContent(parser)
            return RestChannelConsumer { channel: RestChannel? ->
                client.admin().cluster()
                    .execute(BulkPauseReplicationAction.INSTANCE, bulkPauseReplicationRequest, RestToXContentListener(channel))
            }
        }
    }
}
