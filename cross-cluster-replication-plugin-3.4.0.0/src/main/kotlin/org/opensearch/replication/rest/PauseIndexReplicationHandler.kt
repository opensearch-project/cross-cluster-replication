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

import org.opensearch.replication.action.pause.PauseIndexReplicationAction
import org.opensearch.replication.action.pause.PauseIndexReplicationRequest
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.apache.logging.log4j.LogManager
import org.opensearch.transport.client.node.NodeClient
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

class PauseIndexReplicationHandler : BaseRestHandler() {

    companion object {
        private val log = LogManager.getLogger(PauseIndexReplicationHandler::class.java)
    }

    override fun routes(): List<RestHandler.Route> {
        return listOf(RestHandler.Route(RestRequest.Method.POST, "/_plugins/_replication/{index}/_pause"))
    }

    override fun getName(): String {
        return "plugins_index_pause_replicate_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val followIndex = request.param("index")
        val pauseReplicationRequest = if (request.hasContent() || request.hasParam("source")) {
            request.contentOrSourceParamParser().use { parser ->
                PauseIndexReplicationRequest.fromXContent(parser, followIndex)
            }
        } else {
            PauseIndexReplicationRequest(followIndex, ReplicationMetadataManager.CUSTOMER_INITIATED_ACTION)
        }
        return RestChannelConsumer { channel: RestChannel? ->
            client.admin().cluster()
                    .execute(PauseIndexReplicationAction.INSTANCE, pauseReplicationRequest, RestToXContentListener(channel))
        }
    }
}
