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

import org.opensearch.ResourceNotFoundException
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.core.rest.RestStatus
import org.opensearch.replication.task.bulk.BulkReplicationTask
import org.opensearch.replication.util.taskManager
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.transport.client.node.NodeClient

class BulkTaskCancelHandler @Inject constructor(private val clusterService: ClusterService) : BaseRestHandler() {

    companion object {
        private val log = org.apache.logging.log4j.LogManager.getLogger(BulkTaskCancelHandler::class.java)
    }

    override fun routes(): List<RestHandler.Route> =
        listOf(RestHandler.Route(RestRequest.Method.POST, "/_plugins/_replication/_task_cancel/{task_id}"))

    override fun getName() = "plugins_bulk_replication_task_cancel"

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val taskId = request.param("task_id")
            ?: return RestChannelConsumer { channel ->
                channel.sendResponse(BytesRestResponse(RestStatus.BAD_REQUEST, "task_id parameter is required"))
            }
        return RestChannelConsumer { channel ->
            val task = taskManager.getCancellableTasks().values
                .filterIsInstance<BulkReplicationTask>()
                .firstOrNull { taskId.endsWith(":${it.id}") }

            if (task == null) {
                channel.sendResponse(BytesRestResponse(channel, ResourceNotFoundException("Task $taskId not found or already completed")))
                return@RestChannelConsumer
            }

            if (task.isCancelled) {
                channel.sendResponse(BytesRestResponse(channel, ResourceNotFoundException("Task $taskId is already cancelled")))
                return@RestChannelConsumer
            }

            taskManager.cancel(task, "cancelled via _task_cancel API") {}
            val builder = XContentFactory.jsonBuilder()
            builder.startObject().field("acknowledged", true).endObject()
            channel.sendResponse(BytesRestResponse(RestStatus.OK, builder))
        }
    }
}
