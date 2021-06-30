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

import com.amazon.elasticsearch.replication.action.autofollow.UpdateAutoFollowPatternAction
import com.amazon.elasticsearch.replication.action.autofollow.UpdateAutoFollowPatternRequest
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.rest.action.RestToXContentListener

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
                throw ElasticsearchStatusException("Unsupported method ", RestStatus.METHOD_NOT_ALLOWED, request.method())
        }

        val updateRequest = UpdateAutoFollowPatternRequest.fromXContent(request.contentParser(), action)
        return RestChannelConsumer { channel ->
            client.admin().cluster()
                .execute(UpdateAutoFollowPatternAction.INSTANCE, updateRequest, RestToXContentListener(channel))
        }
    }
}
