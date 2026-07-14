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

import org.opensearch.replication.action.resume.ResumeIndexReplicationAction
import org.opensearch.replication.action.resume.ResumeIndexReplicationRequest
import org.apache.logging.log4j.LogManager
import org.opensearch.transport.client.node.NodeClient
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
