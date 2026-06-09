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
import org.opensearch.replication.action.bulk.TransportBulkReplicationAction
import org.opensearch.replication.metadata.state.BulkReplicationTaskMetadata
import org.opensearch.replication.task.bulk.BulkReplicationTask
import org.opensearch.replication.task.bulk.BulkReplicationTaskStatus
import org.opensearch.replication.util.taskManager
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.transport.client.node.NodeClient

class BulkTaskStatusHandler @Inject constructor(private val clusterService: ClusterService) : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> =
        listOf(RestHandler.Route(RestRequest.Method.GET, "/_plugins/_replication/_task_status/{task_id}"))

    override fun getName() = "plugins_bulk_replication_task_status"

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val taskId = request.param("task_id")
            ?: return RestChannelConsumer { channel ->
                channel.sendResponse(BytesRestResponse(RestStatus.BAD_REQUEST, "task_id parameter is required"))
            }
        val pretty = request.paramAsBoolean("pretty", false)
        return RestChannelConsumer { channel ->

            fun respond(status: BulkReplicationTaskStatus) {
                val builder = XContentFactory.jsonBuilder().also { if (pretty) it.prettyPrint() }
                status.toXContent(builder, null)
                channel.sendResponse(BytesRestResponse(RestStatus.OK, builder))
            }

            // 1. completed task cache
            TransportBulkReplicationAction.getCompletedStatus(taskId)?.let { respond(it); return@RestChannelConsumer }

            // 2. cluster state
            clusterService.state().metadata
                .custom<BulkReplicationTaskMetadata>(BulkReplicationTaskMetadata.NAME)?.taskState
                ?.takeIf { it.taskId == taskId }
                ?.let { respond(BulkReplicationTaskStatus.fromTaskState(it)); return@RestChannelConsumer }

            // 3. TaskManager — task still running on this node
            val numericId = taskId.split(":").getOrNull(1)?.toLongOrNull()
            if (numericId != null) {
                taskManager.getCancellableTasks().values
                    .filterIsInstance<BulkReplicationTask>()
                    .firstOrNull { it.id == numericId }
                    ?.let { respond(it.getStatus() as BulkReplicationTaskStatus); return@RestChannelConsumer }
            }

            // 4. not found
            channel.sendResponse(BytesRestResponse(channel, ResourceNotFoundException("Task $taskId not found or already completed")))
        }
    }
}
