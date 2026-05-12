package org.opensearch.replication.rest

import org.apache.logging.log4j.LogManager
import org.opensearch.transport.client.node.NodeClient
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.replication.action.stats.AutoFollowStatsAction
import org.opensearch.replication.action.stats.AutoFollowStatsRequest
import org.opensearch.replication.action.stats.AutoFollowStatsResponses
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestResponse
import org.opensearch.core.rest.RestStatus
import org.opensearch.rest.action.RestResponseListener
import java.io.IOException

class AutoFollowStatsHandler : BaseRestHandler() {
    companion object {
        private val log = LogManager.getLogger(AutoFollowStatsHandler::class.java)
    }

    override fun routes(): List<RestHandler.Route> {
        return listOf(RestHandler.Route(RestRequest.Method.GET, "/_plugins/_replication/autofollow_stats"))
    }

    override fun getName(): String {
        return "plugins_autofollow_replication_stats"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val statsRequest = AutoFollowStatsRequest()
        return RestChannelConsumer { channel: RestChannel? ->
            client.admin().cluster()
                    .execute(AutoFollowStatsAction.INSTANCE, statsRequest, object : RestResponseListener<AutoFollowStatsResponses>(channel) {
                        @Throws(Exception::class)
                        override fun buildResponse(nodesStatsResponses: AutoFollowStatsResponses): RestResponse? {
                            val builder: XContentBuilder = XContentFactory.jsonBuilder().prettyPrint()
                            return BytesRestResponse(RestStatus.OK, nodesStatsResponses.toXContent(builder, ToXContent.EMPTY_PARAMS))
                        }
                    })
        }
    }
}

