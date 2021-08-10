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

import org.opensearch.replication.action.update.UpdateIndexReplicationAction
import org.opensearch.replication.action.update.UpdateIndexReplicationRequest
import org.opensearch.replication.task.index.IndexReplicationExecutor.Companion.log
import org.opensearch.action.support.IndicesOptions
import org.opensearch.client.Requests
import org.opensearch.client.node.NodeClient
import org.opensearch.common.Strings
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
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
