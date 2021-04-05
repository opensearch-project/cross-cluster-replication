package com.amazon.elasticsearch.replication.rest


import com.amazon.elasticsearch.replication.action.status.IndexReplicationStatusAction
import com.amazon.elasticsearch.replication.action.status.IndexReplicationStatusRequest
import org.apache.logging.log4j.LogManager
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.RestChannel
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.action.RestToXContentListener
import java.io.IOException

class ReplicationStatusHandler : BaseRestHandler() {

    companion object {
        private val log = LogManager.getLogger(ReplicationStatusHandler::class.java)
    }

    override fun routes(): List<RestHandler.Route> {
        return listOf(RestHandler.Route(RestRequest.Method.GET, "/_opendistro/{index}/_replication/_status"))
    }

    override fun getName(): String {
        return "opendistro_replication_status"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
                request.contentOrSourceParamParser().use { parser ->
                        val followIndex = request.param("index")
                    val indexReplicationStatusRequest = IndexReplicationStatusRequest.fromXContent(parser, followIndex)
                    return RestChannelConsumer { channel: RestChannel? ->
                                client.admin().cluster()
                                        .execute(IndexReplicationStatusAction.INSTANCE, indexReplicationStatusRequest, RestToXContentListener(channel))
                            }
                }
    }

}