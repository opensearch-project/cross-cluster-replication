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

import org.opensearch.replication.action.autofollow.UpdateAutoFollowPatternAction
import org.opensearch.replication.action.autofollow.UpdateAutoFollowPatternRequest
import org.opensearch.OpenSearchStatusException
import org.opensearch.transport.client.node.NodeClient
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.core.rest.RestStatus
import org.opensearch.rest.action.RestToXContentListener

class UpdateAutoFollowPatternsHandler : BaseRestHandler() {

    companion object {
        const val PATH = "/_plugins/_replication/_autofollow"
    }

    override fun routes(): List<RestHandler.Route> {
        return listOf(RestHandler.Route(RestRequest.Method.POST, PATH),
            RestHandler.Route(RestRequest.Method.DELETE, PATH))
    }

    override fun getName() = "plugins_replication_autofollow_update"

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val action = when {
            request.method() == RestRequest.Method.POST -> UpdateAutoFollowPatternRequest.Action.ADD
            request.method() == RestRequest.Method.DELETE -> UpdateAutoFollowPatternRequest.Action.REMOVE
            // Should not be reached unless someone updates the restController with a new method but forgets to add it here.
            else ->
                throw OpenSearchStatusException("Unsupported method ", RestStatus.METHOD_NOT_ALLOWED, request.method())
        }

        val updateRequest = UpdateAutoFollowPatternRequest.fromXContent(request.contentParser(), action)
        return RestChannelConsumer { channel ->
            client.admin().cluster()
                .execute(UpdateAutoFollowPatternAction.INSTANCE, updateRequest, RestToXContentListener(channel))
        }
    }
}
