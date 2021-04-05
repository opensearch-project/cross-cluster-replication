/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.elasticsearch.replication.rest

import com.amazon.elasticsearch.replication.action.index.ReplicateIndexAction
import com.amazon.elasticsearch.replication.action.index.ReplicateIndexRequest
import org.apache.logging.log4j.LogManager
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestChannel
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.action.RestToXContentListener
import java.io.IOException

class ReplicateIndexHandler : BaseRestHandler() {

    companion object {
        private val log = LogManager.getLogger(ReplicateIndexHandler::class.java)
    }

    override fun routes(): List<RestHandler.Route> {
        return listOf(RestHandler.Route(RestRequest.Method.PUT, "/_opendistro/_replication/{index}/_start"))
    }

    override fun getName(): String {
        return "opendistro_index_start_replicate_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        request.contentOrSourceParamParser().use { parser ->
            val followerIndex = request.param("index")
            val followIndexRequest = ReplicateIndexRequest.fromXContent(parser, followerIndex)
            followIndexRequest.waitForRestore = request.paramAsBoolean("wait_for_restore", false)
            return RestChannelConsumer {
                channel: RestChannel? -> client.admin().cluster()
                    .execute(ReplicateIndexAction.INSTANCE, followIndexRequest, RestToXContentListener(channel))
            }
        }
    }
}
