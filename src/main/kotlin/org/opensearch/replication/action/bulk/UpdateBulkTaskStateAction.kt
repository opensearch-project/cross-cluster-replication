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

package org.opensearch.replication.action.bulk

import org.opensearch.OpenSearchStatusException
import org.opensearch.core.rest.RestStatus
import org.opensearch.action.ActionType
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.clustermanager.AcknowledgedRequest
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction
import org.opensearch.cluster.AckedClusterStateUpdateTask
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import org.opensearch.replication.metadata.state.BulkReplicationTaskMetadata
import org.opensearch.replication.metadata.state.BulkTaskState
import java.io.IOException

class UpdateBulkTaskStateAction private constructor() :
    ActionType<AcknowledgedResponse>(NAME, ::AcknowledgedResponse) {
    companion object {
        const val NAME = "cluster:admin/plugins/replication/bulk/state/update"
        val INSTANCE = UpdateBulkTaskStateAction()
    }
}

class UpdateBulkTaskStateRequest : AcknowledgedRequest<UpdateBulkTaskStateRequest> {
    val taskState: BulkTaskState?

    // taskState == null means clear the lock (task completed or was cancelled).
    constructor(taskState: BulkTaskState?) { this.taskState = taskState }

    constructor(inp: StreamInput) : super(inp) {
        taskState = if (inp.readBoolean()) BulkTaskState(inp) else null
    }

    override fun validate() = null

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        if (taskState != null) { out.writeBoolean(true); taskState.writeTo(out) }
        else out.writeBoolean(false)
    }
}

class TransportUpdateBulkTaskStateAction @Inject constructor(
    transportService: TransportService,
    clusterService: ClusterService,
    threadPool: ThreadPool,
    actionFilters: ActionFilters,
    indexNameExpressionResolver: IndexNameExpressionResolver
) : TransportClusterManagerNodeAction<UpdateBulkTaskStateRequest, AcknowledgedResponse>(
    UpdateBulkTaskStateAction.NAME, transportService, clusterService, threadPool,
    actionFilters, ::UpdateBulkTaskStateRequest, indexNameExpressionResolver
) {
    override fun checkBlock(request: UpdateBulkTaskStateRequest, state: ClusterState): ClusterBlockException? =
        state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)

    override fun clusterManagerOperation(request: UpdateBulkTaskStateRequest, state: ClusterState,
                                         listener: ActionListener<AcknowledgedResponse>) {
        clusterService.submitStateUpdateTask("update_bulk_task_state",
            object : AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {
                override fun execute(currentState: ClusterState): ClusterState {
                    // Guard against concurrent bulk task submissions. Only checked on the initial
                    // claim (taskId is empty). Progress updates from the running task carry the
                    // real taskId and bypass this check.
                    if (request.taskState != null && request.taskState.taskId.isEmpty()) {
                        val existing = currentState.metadata
                            .custom<BulkReplicationTaskMetadata>(BulkReplicationTaskMetadata.NAME)?.taskState
                        if (existing != null && existing.numPending > 0) {
                            throw OpenSearchStatusException(
                                "A bulk replication task is already running. Only one bulk task is allowed at a time.", RestStatus.CONFLICT)
                        }
                    }
                    val newMetadata = Metadata.builder(currentState.metadata)
                        .putCustom(BulkReplicationTaskMetadata.NAME, BulkReplicationTaskMetadata(request.taskState))
                    return ClusterState.builder(currentState).metadata(newMetadata).build()
                }
                override fun newResponse(acknowledged: Boolean) = AcknowledgedResponse(acknowledged)
            })
    }

    override fun executor() = ThreadPool.Names.SAME

    @Throws(IOException::class)
    override fun read(inp: StreamInput) = AcknowledgedResponse(inp)
}
