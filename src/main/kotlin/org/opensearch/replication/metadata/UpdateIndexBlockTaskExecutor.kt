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
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.suspendCancellableCoroutine
import org.apache.logging.log4j.LogManager
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.ClusterStateTaskConfig
import org.opensearch.cluster.ClusterStateTaskExecutor
import org.opensearch.cluster.ClusterStateTaskListener
import org.opensearch.cluster.block.ClusterBlocks
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.Priority
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Batched executor for index block add/remove operations. Multiple concurrent UpdateIndexBlockRequest
 * submissions (with the same source string) are coalesced into a single cluster-state publish,
 * reducing N publishes to ~1 when fired concurrently (e.g., from bulk start/stop flows).
 *
 * The existing single-index flow (TransportUpddateIndexBlockAction + UpdateIndexBlockTask) is NOT
 * modified. This executor is used exclusively by the bulk API via [submitBatch].
 */
class UpdateIndexBlockTaskExecutor private constructor()
    : ClusterStateTaskExecutor<UpdateIndexBlockRequest> {

    companion object {
        private val log = LogManager.getLogger(UpdateIndexBlockTaskExecutor::class.java)
        val INSTANCE = UpdateIndexBlockTaskExecutor()

        /**
         * Submits index block updates for multiple indices concurrently. All submissions hit the
         * same batched executor with the same source string, so the cluster-service coalesces them
         * into ~1 cluster-state publish. Each coroutine suspends until its request is processed.
         *
         * @return set of index names that were successfully updated
         */
        /**
         * Fires all index block updates concurrently — same executor + same source string means
         * TaskBatcher coalesces them into ~1 cluster-state publish. Block add/remove is idempotent
         * so execute() always succeeds all tasks.
         */
        suspend fun submitBatch(
            clusterService: ClusterService,
            indices: List<String>,
            updateType: IndexBlockUpdateType
        ): Set<String> {
            if (indices.isEmpty()) return emptySet()

            val succeeded = java.util.concurrent.CopyOnWriteArrayList<String>()
            coroutineScope {
                indices.map { index ->
                    async {
                        try {
                            val request = UpdateIndexBlockRequest(index, updateType)
                            submitOne(clusterService, request)
                            succeeded.add(index)
                        } catch (e: Exception) {
                            log.debug("Failed to update index block for index=$index (${updateType}): ${e.message}")
                        }
                    }
                }.map { it.await() }
            }
            return succeeded.toSet()
        }

        private suspend fun submitOne(clusterService: ClusterService, request: UpdateIndexBlockRequest) {
            suspendCancellableCoroutine<ClusterState> { continuation ->
                clusterService.submitStateUpdateTask(
                    "update-index-block",
                    request,
                    ClusterStateTaskConfig.build(Priority.NORMAL),
                    INSTANCE as ClusterStateTaskExecutor<UpdateIndexBlockRequest>,
                    object : ClusterStateTaskListener {
                        override fun onFailure(source: String, e: java.lang.Exception) {
                            continuation.resumeWithException(e)
                        }

                        override fun clusterStateProcessed(source: String?, oldState: ClusterState?, newState: ClusterState) {
                            continuation.resume(newState)
                        }
                    }
                )
            }
        }
    }

    override fun execute(currentState: ClusterState, tasks: List<UpdateIndexBlockRequest>)
            : ClusterStateTaskExecutor.ClusterTasksResult<UpdateIndexBlockRequest> {
        log.debug("Executing batched index block update for ${tasks.size} requests")

        val blocksBuilder = ClusterBlocks.builder().blocks(currentState.blocks)
        var changed = false

        for (request in tasks) {
            when (request.updateType) {
                IndexBlockUpdateType.ADD_BLOCK -> {
                    if (!currentState.blocks.hasIndexBlock(request.indexName, INDEX_REPLICATION_BLOCK)) {
                        blocksBuilder.addIndexBlock(request.indexName, INDEX_REPLICATION_BLOCK)
                        changed = true
                    }
                }
                IndexBlockUpdateType.REMOVE_BLOCK -> {
                    if (currentState.blocks.hasIndexBlock(request.indexName, INDEX_REPLICATION_BLOCK)) {
                        blocksBuilder.removeIndexBlock(request.indexName, INDEX_REPLICATION_BLOCK)
                        changed = true
                    }
                }
            }
        }

        val newState = if (changed) {
            ClusterState.Builder(currentState).blocks(blocksBuilder).build()
        } else {
            currentState
        }

        return ClusterStateTaskExecutor.ClusterTasksResult.builder<UpdateIndexBlockRequest>()
            .successes(tasks).build(newState)
    }
}
