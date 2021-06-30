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

import com.amazon.elasticsearch.replication.action.update.UpdateIndexReplicationAction
import com.amazon.elasticsearch.replication.action.update.UpdateIndexReplicationRequest
import com.amazon.elasticsearch.replication.task.index.IndexReplicationExecutor.Companion.log
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.client.Requests
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.Strings
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestChannel
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.action.RestToXContentListener
import java.io.IOException

class UpdateIndexHandler : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> {
        return listOf(RestHandler.Route(RestRequest.Method.PUT, "/_plugins/_replication/{index}/_update"))
    }

    override fun getName(): String {
        return "plugins_index_update_replicate_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer? {
        val followIndex = request.param("index")
        log.info("Update Setting requested for $followIndex")
        val updateSettingsRequest = Requests.updateSettingsRequest(*Strings.splitStringByCommaToArray(request.param("index")))
        updateSettingsRequest.timeout(request.paramAsTime("timeout", updateSettingsRequest.timeout()))
        updateSettingsRequest.masterNodeTimeout(request.paramAsTime("master_timeout", updateSettingsRequest.masterNodeTimeout()))
        updateSettingsRequest.indicesOptions(IndicesOptions.fromRequest(request, updateSettingsRequest.indicesOptions()))
        updateSettingsRequest.fromXContent(request.contentParser())
        val updateIndexReplicationRequest = UpdateIndexReplicationRequest(followIndex, updateSettingsRequest.settings() )
        return RestChannelConsumer { channel: RestChannel? ->
            client.admin().cluster()
                    .execute(UpdateIndexReplicationAction.INSTANCE, updateIndexReplicationRequest, RestToXContentListener(channel))
        }
    }
}
