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

import org.opensearch.replication.action.update.UpdateIndexReplicationAction
import org.opensearch.replication.action.update.UpdateIndexReplicationRequest
import org.opensearch.replication.task.index.IndexReplicationExecutor.Companion.log
import org.opensearch.action.support.IndicesOptions
import org.opensearch.transport.client.Requests
import org.opensearch.transport.client.node.NodeClient
import org.opensearch.core.common.Strings
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
        updateSettingsRequest.clusterManagerNodeTimeout(request.paramAsTime("master_timeout", updateSettingsRequest.clusterManagerNodeTimeout()))
        updateSettingsRequest.indicesOptions(IndicesOptions.fromRequest(request, updateSettingsRequest.indicesOptions()))
        updateSettingsRequest.fromXContent(request.contentParser())
        val updateIndexReplicationRequest = UpdateIndexReplicationRequest(followIndex, updateSettingsRequest.settings() )
        return RestChannelConsumer { channel: RestChannel? ->
            client.admin().cluster()
                    .execute(UpdateIndexReplicationAction.INSTANCE, updateIndexReplicationRequest, RestToXContentListener(channel))
        }
    }
}
