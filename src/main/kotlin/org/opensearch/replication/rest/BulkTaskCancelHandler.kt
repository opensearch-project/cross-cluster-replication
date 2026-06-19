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
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.tasks.TaskId
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.transport.client.node.NodeClient

class BulkTaskCancelHandler @Inject constructor(private val clusterService: ClusterService) : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> =
        listOf(RestHandler.Route(RestRequest.Method.POST, "/_plugins/_replication/_task_cancel/{task_id}"))

    override fun getName() = "plugins_bulk_replication_task_cancel"

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val taskId = request.param("task_id")
            ?: return RestChannelConsumer { channel ->
                channel.sendResponse(BytesRestResponse(RestStatus.BAD_REQUEST, "task_id parameter is required"))
            }
        return RestChannelConsumer { channel ->
            val parts = taskId.split(":")
            if (parts.size != 2 || parts[1].toLongOrNull() == null) {
                channel.sendResponse(BytesRestResponse(RestStatus.BAD_REQUEST, "Invalid task_id format"))
                return@RestChannelConsumer
            }

            val cancelRequest = CancelTasksRequest()
            cancelRequest.setTaskId(TaskId(parts[0], parts[1].toLong()))
            cancelRequest.setReason("cancelled via _task_cancel API")

            client.admin().cluster().cancelTasks(cancelRequest, object : ActionListener<CancelTasksResponse> {
                override fun onResponse(response: CancelTasksResponse) {
                    if (response.tasks.isEmpty()) {
                        channel.sendResponse(BytesRestResponse(channel,
                            ResourceNotFoundException("Task $taskId not found or already completed")))
                    } else {
                        val builder = XContentFactory.jsonBuilder()
                        builder.startObject().field("acknowledged", true).endObject()
                        channel.sendResponse(BytesRestResponse(RestStatus.OK, builder))
                    }
                }
                override fun onFailure(e: Exception) {
                    channel.sendResponse(BytesRestResponse(channel, e))
                }
            })
        }
    }
}
