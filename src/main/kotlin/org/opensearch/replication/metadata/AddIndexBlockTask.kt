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

package org.opensearch.replication.metadata

import org.opensearch.replication.action.index.block.UpdateIndexBlockRequest
import org.opensearch.action.ActionListener
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.cluster.AckedClusterStateUpdateTask
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.block.ClusterBlock
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.block.ClusterBlocks
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.collect.ImmutableOpenMap
import org.opensearch.index.IndexNotFoundException
import org.opensearch.rest.RestStatus
import java.util.*


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
        /* Follower index deletion is allowed in the absence of Metadata block. */
        // TODO: Add METADATA_WRITE to the list of blocked actions once we have a way for the replication tasks
        //       to make metadata changes like updating document mappings.
        EnumSet.of(ClusterBlockLevel.WRITE))

/* This function checks the local cluster state to see if given
    index is blocked with given level with any block other than
    our own INDEX_REPLICATION_BLOCK
*/
fun checkIfIndexBlockedWithLevel(clusterService: ClusterService,
                                 indexName: String,
                                 clusterBlockLevel: ClusterBlockLevel) {
    clusterService.state().routingTable.index(indexName) ?:
    throw IndexNotFoundException("Index with name:$indexName doesn't exist")
    val writeIndexBlockMap : ImmutableOpenMap<String, Set<ClusterBlock>> = clusterService.state().blocks()
            .indices(clusterBlockLevel)
    if (!writeIndexBlockMap.containsKey(indexName))
        return
    val clusterBlocksSet : Set<ClusterBlock> = writeIndexBlockMap.get(indexName)
    if (clusterBlocksSet.contains(INDEX_REPLICATION_BLOCK)
            && clusterBlocksSet.size > 1)
        throw ClusterBlockException(clusterBlocksSet)
}

class AddIndexBlockTask(val request: UpdateIndexBlockRequest, listener: ActionListener<AcknowledgedResponse>) :
        AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener)
{
    override fun execute(currentState: ClusterState): ClusterState {
        val newState = ClusterState.builder(currentState)

        if (!currentState.blocks.hasIndexBlock(request.indexName, INDEX_REPLICATION_BLOCK)) {
            val newBlocks = ClusterBlocks.builder().blocks(currentState.blocks)
                    .addIndexBlock(request.indexName, INDEX_REPLICATION_BLOCK)
            newState.blocks(newBlocks)
        }
        return newState.build()
    }

    override fun newResponse(acknowledged: Boolean) = AcknowledgedResponse(acknowledged)
}
