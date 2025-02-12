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

package org.opensearch.replication.metadata

import org.opensearch.replication.action.index.block.IndexBlockUpdateType
import org.opensearch.replication.action.index.block.UpdateIndexBlockRequest
import org.opensearch.core.action.ActionListener
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.cluster.AckedClusterStateUpdateTask
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.block.ClusterBlock
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.block.ClusterBlocks
import org.opensearch.cluster.service.ClusterService
import org.opensearch.index.IndexNotFoundException
import org.opensearch.core.rest.RestStatus
import java.util.Collections
import java.util.EnumSet


/* This is our custom index block to prevent changes to follower
        index while replication is in progress.
         */
val INDEX_REPLICATION_BLOCK = ClusterBlock(
        1000,
        "index read-only(cross-cluster-replication)",
        false,
        false,
        false,
        RestStatus.FORBIDDEN,
        EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE))

/* This function checks the local cluster state to see if given
    index is blocked with given level with any block other than
    our own INDEX_REPLICATION_BLOCK
*/
fun checkIfIndexBlockedWithLevel(clusterService: ClusterService,
                                 indexName: String,
                                 clusterBlockLevel: ClusterBlockLevel) {
    clusterService.state().routingTable.index(indexName) ?:
    throw IndexNotFoundException("Index with name:$indexName doesn't exist")
    val writeIndexBlockMap : Map<String, Set<ClusterBlock>> = clusterService.state().blocks()
            .indices(clusterBlockLevel)
    if (!writeIndexBlockMap.containsKey(indexName))
        return
    val clusterBlocksSet : Set<ClusterBlock> = writeIndexBlockMap.getOrDefault(indexName, Collections.emptySet())
    if (clusterBlocksSet.contains(INDEX_REPLICATION_BLOCK)
            && clusterBlocksSet.size > 1)
        throw ClusterBlockException(clusterBlocksSet)
}

class UpdateIndexBlockTask(val request: UpdateIndexBlockRequest, listener: ActionListener<AcknowledgedResponse>) :
        AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener)
{
    override fun execute(currentState: ClusterState): ClusterState {
        val newState = ClusterState.builder(currentState)
        when(request.updateType) {
            IndexBlockUpdateType.ADD_BLOCK -> {
                if (!currentState.blocks.hasIndexBlock(request.indexName, INDEX_REPLICATION_BLOCK)) {
                    val newBlocks = ClusterBlocks.builder().blocks(currentState.blocks)
                        .addIndexBlock(request.indexName, INDEX_REPLICATION_BLOCK)
                    newState.blocks(newBlocks)
                }
            }
            IndexBlockUpdateType.REMOVE_BLOCK -> {
                val newBlocks = ClusterBlocks.builder().blocks(currentState.blocks)
                    .removeIndexBlock(request.indexName, INDEX_REPLICATION_BLOCK)
                newState.blocks(newBlocks)
            }
        }
        return newState.build()
    }

    override fun newResponse(acknowledged: Boolean) = AcknowledgedResponse(acknowledged)
}
