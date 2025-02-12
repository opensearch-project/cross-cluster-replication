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

import org.opensearch.replication.action.stop.StopIndexReplicationAction
import org.opensearch.replication.action.stop.StopIndexReplicationRequest
import org.opensearch.transport.client.node.NodeClient
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

class StopIndexReplicationHandler : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> {
        return listOf(RestHandler.Route(RestRequest.Method.POST, "/_plugins/_replication/{index}/_stop"))
    }

    override fun getName(): String {
        return "plugins_index_stop_replicate_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        request.contentOrSourceParamParser().use { parser ->
            val followIndex = request.param("index")
            val stopReplicationRequest = StopIndexReplicationRequest.fromXContent(parser, followIndex)
            return RestChannelConsumer { channel: RestChannel? ->
                client.admin().cluster()
                        .execute(StopIndexReplicationAction.INSTANCE, stopReplicationRequest, RestToXContentListener(channel))
            }
        }
    }
}
