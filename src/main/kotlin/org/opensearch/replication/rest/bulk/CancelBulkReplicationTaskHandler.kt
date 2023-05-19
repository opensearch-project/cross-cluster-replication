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

package org.opensearch.replication.rest.bulk

import org.opensearch.client.node.NodeClient
import org.opensearch.replication.action.bulk.CancelBulkReplicationTaskAction
import org.opensearch.replication.action.bulk.CancelBulkReplicationTaskRequest
import org.opensearch.replication.action.bulk.stop.BulkStopReplicationAction
import org.opensearch.replication.action.bulk.stop.BulkStopReplicationRequest
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

class CancelBulkReplicationTaskHandler : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> {
        return listOf(RestHandler.Route(RestRequest.Method.POST, "/_plugins/_replication/_bulk_cancel"))
    }

    override fun getName(): String {
        return "plugins_index_bulk_cancel_replication_task"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        request.contentOrSourceParamParser().use { parser ->

            val cancelBulkReplicationTaskRequest = CancelBulkReplicationTaskRequest.fromXContent(parser)
            return RestChannelConsumer { channel: RestChannel? ->
                client.admin().cluster()
                    .execute(CancelBulkReplicationTaskAction.INSTANCE, cancelBulkReplicationTaskRequest, RestToXContentListener(channel))
            }
        }
    }
}
