package org.opensearch.replication.rest

import com.amazon.elasticsearch.replication.action.stats.LeaderStatsAction
import com.amazon.elasticsearch.replication.action.stats.LeaderStatsRequest
import com.amazon.elasticsearch.replication.action.stats.LeaderStatsResponse
import org.apache.logging.log4j.LogManager
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.BytesRestResponse
import org.elasticsearch.rest.RestChannel
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestResponse
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.rest.action.RestResponseListener
import java.io.IOException

class LeaderStatsHandler : BaseRestHandler() {
    companion object {
        private val log = LogManager.getLogger(LeaderStatsHandler::class.java)
    }

    override fun routes(): List<RestHandler.Route> {
        return listOf(RestHandler.Route(RestRequest.Method.GET, "/_plugins/_replication/leader_stats"))
    }

    override fun getName(): String {
        return "plugins_leader_replication_stats"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val statsRequest = LeaderStatsRequest()
        return RestChannelConsumer { channel: RestChannel? ->
            client.admin().cluster()
                    .execute(LeaderStatsAction.INSTANCE, statsRequest, object : RestResponseListener<LeaderStatsResponse>(channel) {
                        @Throws(Exception::class)
                        override fun buildResponse(nodesStatsResponse: LeaderStatsResponse): RestResponse? {
                            val builder: XContentBuilder = XContentFactory.jsonBuilder().prettyPrint()
                            return BytesRestResponse(RestStatus.OK, nodesStatsResponse.toXContent(builder, ToXContent.EMPTY_PARAMS))
                        }
                    })
        }
    }
}
