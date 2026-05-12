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


import org.opensearch.replication.action.status.ReplicationStatusAction
import org.opensearch.replication.action.status.ShardInfoRequest
import org.apache.logging.log4j.LogManager
import org.opensearch.transport.client.node.NodeClient
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

class ReplicationStatusHandler : BaseRestHandler() {

    companion object {
        private val log = LogManager.getLogger(ReplicationStatusHandler::class.java)
    }

    override fun routes(): List<RestHandler.Route> {
        return listOf(RestHandler.Route(RestRequest.Method.GET, "/_plugins/_replication/{index}/_status"))
    }

    override fun getName(): String {
        return "plugins_replication_status"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val index = request.param("index", "")
        var isVerbose = (request.paramAsBoolean("verbose", false))
        val indexReplicationStatusRequest = ShardInfoRequest(index,isVerbose)
        return RestChannelConsumer {
            channel ->
            client.execute(ReplicationStatusAction.INSTANCE, indexReplicationStatusRequest, RestToXContentListener(channel))
        }
    }
}
