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

import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.clustermanager.AcknowledgedRequest
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction
import org.opensearch.cluster.AckedClusterStateUpdateTask
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.block.ClusterBlocks
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.replication.ReplicationPlugin.Companion.REPLICATED_INDEX_SETTING
import org.opensearch.replication.metadata.INDEX_REPLICATION_BLOCK
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService

class BulkStopClusterManagerRequest : org.opensearch.action.support.clustermanager.AcknowledgedRequest<BulkStopClusterManagerRequest> {
    val indices: List<String>
    constructor(indices: List<String>) : super() { this.indices = indices }
    constructor(inp: StreamInput) : super(inp) { this.indices = inp.readStringList() }
    override fun writeTo(out: StreamOutput) { super.writeTo(out); out.writeStringCollection(indices) }
    override fun validate() = null
}

class TransportBulkClusterManagerNodeAction @Inject constructor(
    transportService: TransportService, clusterService: ClusterService, threadPool: ThreadPool,
    actionFilters: ActionFilters, indexNameExpressionResolver: IndexNameExpressionResolver
) : TransportClusterManagerNodeAction<BulkStopClusterManagerRequest, AcknowledgedResponse>(
    BatchStopClusterStateAction.NAME, transportService, clusterService, threadPool,
    actionFilters, ::BulkStopClusterManagerRequest, indexNameExpressionResolver
) {
    override fun executor() = ThreadPool.Names.SAME
    override fun read(inp: StreamInput) = AcknowledgedResponse(inp)
    override fun checkBlock(req: BulkStopClusterManagerRequest, state: ClusterState): ClusterBlockException? =
        state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)

    override fun clusterManagerOperation(req: BulkStopClusterManagerRequest, state: ClusterState, listener: ActionListener<AcknowledgedResponse>) {
        clusterService.submitStateUpdateTask("bulk_stop_replication", object :
            AckedClusterStateUpdateTask<AcknowledgedResponse>(req, listener) {
            override fun execute(cs: ClusterState): ClusterState {
                val blocks = ClusterBlocks.builder().blocks(cs.blocks)
                val md = Metadata.builder(cs.metadata)
                for (index in req.indices) {
                    if (cs.blocks.hasIndexBlock(index, INDEX_REPLICATION_BLOCK)) blocks.removeIndexBlock(index, INDEX_REPLICATION_BLOCK)
                    cs.metadata.index(index)?.let { im ->
                        if (im.settings[REPLICATED_INDEX_SETTING.key] != null)
                            md.put(IndexMetadata.builder(im).settings(Settings.builder().put(im.settings).putNull(REPLICATED_INDEX_SETTING.key)).settingsVersion(1 + im.settingsVersion))
                    }
                }
                return ClusterState.builder(cs).blocks(blocks).metadata(md).build()
            }
            override fun newResponse(acknowledged: Boolean) = AcknowledgedResponse(acknowledged)
        })
    }
}
