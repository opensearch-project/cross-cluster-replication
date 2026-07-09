package org.opensearch.replication.rest

import org.apache.logging.log4j.LogManager
import org.opensearch.transport.client.node.NodeClient
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.replication.action.stats.FollowerStatsAction
import org.opensearch.replication.action.stats.FollowerStatsRequest
import org.opensearch.replication.action.stats.FollowerStatsResponse
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

class FollowerStatsHandler : BaseRestHandler() {
    companion object {
        private val log = LogManager.getLogger(FollowerStatsHandler::class.java)
    }

    override fun routes(): List<RestHandler.Route> {
        return listOf(RestHandler.Route(RestRequest.Method.GET, "/_plugins/_replication/follower_stats"))
    }

    override fun getName(): String {
        return "plugins_follower_replication_stats"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val statsRequest = FollowerStatsRequest()
        return RestChannelConsumer { channel: RestChannel? ->
            client.admin().cluster()
                    .execute(FollowerStatsAction.INSTANCE, statsRequest, object : RestResponseListener<FollowerStatsResponse>(channel) {
                        @Throws(Exception::class)
                        override fun buildResponse(nodesStatsResponse: FollowerStatsResponse): RestResponse? {
                            val builder: XContentBuilder = XContentFactory.jsonBuilder().prettyPrint()
                            return BytesRestResponse(RestStatus.OK, nodesStatsResponse.toXContent(builder, ToXContent.EMPTY_PARAMS))
                        }
                    })
        }
    }
}