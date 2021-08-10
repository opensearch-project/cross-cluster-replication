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

package org.opensearch.replication.rest

import org.opensearch.replication.action.resume.ResumeIndexReplicationAction
import org.opensearch.replication.action.resume.ResumeIndexReplicationRequest
import org.apache.logging.log4j.LogManager
import org.opensearch.client.node.NodeClient
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

class ResumeIndexReplicationHandler : BaseRestHandler() {

    companion object {
        private val log = LogManager.getLogger(ResumeIndexReplicationHandler::class.java)
    }

    override fun routes(): List<RestHandler.Route> {
        return listOf(RestHandler.Route(RestRequest.Method.POST, "/_plugins/_replication/{index}/_resume"))
    }

    override fun getName(): String {
        return "plugins_index_resume_replicate_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        request.contentOrSourceParamParser().use { parser ->
            val followIndex = request.param("index")
            val resumeReplicationRequest = ResumeIndexReplicationRequest.fromXContent(parser, followIndex)
            return RestChannelConsumer { channel: RestChannel? ->
                client.admin().cluster()
                        .execute(ResumeIndexReplicationAction.INSTANCE, resumeReplicationRequest, RestToXContentListener(channel))
            }
        }
    }
}
