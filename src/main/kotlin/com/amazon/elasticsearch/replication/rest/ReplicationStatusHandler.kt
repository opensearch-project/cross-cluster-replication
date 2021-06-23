package com.amazon.elasticsearch.replication.rest


import com.amazon.elasticsearch.replication.action.status.ReplicationStatusAction
import com.amazon.elasticsearch.replication.action.status.ReplicationStatusRequest
import org.apache.logging.log4j.LogManager
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.action.RestToXContentListener
import java.io.IOException

class ReplicationStatusHandler : BaseRestHandler() {

    companion object {
        private val log = LogManager.getLogger(ReplicationStatusHandler::class.java)
    }

    override fun routes(): List<RestHandler.Route> {
        return listOf(RestHandler.Route(RestRequest.Method.GET, "/_opendistro/_replication/{index}/_status"))
    }

    override fun getName(): String {
        return "opendistro_replication_status"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val index = request.param("index")
        val indexReplicationStatusRequest = ReplicationStatusRequest(index)
        return RestChannelConsumer {
            channel ->
            client.execute(ReplicationStatusAction.INSTANCE, indexReplicationStatusRequest, RestToXContentListener(channel))
        }
    }

}