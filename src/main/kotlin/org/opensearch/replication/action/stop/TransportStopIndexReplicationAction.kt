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

package org.opensearch.replication.action.stop

import org.opensearch.commons.replication.action.ReplicationActions.STOP_REPLICATION_ACTION_NAME
import org.opensearch.commons.replication.action.StopIndexReplicationRequest
import org.opensearch.replication.ReplicationPlugin.Companion.REPLICATED_INDEX_SETTING
import org.opensearch.replication.action.index.block.IndexBlockUpdateType
import org.opensearch.replication.action.index.block.UpdateIndexBlockAction
import org.opensearch.replication.action.index.block.UpdateIndexBlockRequest
import org.opensearch.replication.metadata.INDEX_REPLICATION_BLOCK
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.metadata.ReplicationOverallState
import org.opensearch.replication.metadata.UpdateMetadataAction
import org.opensearch.replication.metadata.UpdateMetadataRequest
import org.opensearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import org.opensearch.replication.metadata.state.getReplicationStateParamsForIndex
import org.opensearch.replication.task.cleanup.TaskCleanupManager
import org.opensearch.replication.task.cleanup.StaleArtifactDetector
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.suspendExecute
import org.opensearch.replication.util.suspending
import org.opensearch.replication.util.waitForClusterStateUpdate
import org.opensearch.replication.util.stackTraceToString
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchException
import org.opensearch.core.action.ActionListener
import org.opensearch.action.admin.indices.open.OpenIndexRequest
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.Requests
import org.opensearch.cluster.AckedClusterStateUpdateTask
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.RestoreInProgress
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.block.ClusterBlocks
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.common.settings.Settings
import org.opensearch.persistent.PersistentTasksService
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import java.io.IOException

/*
 The classes StopIndexReplicationRequest and StopIndexReplicationAction have been moved from ccr to common-utils
 and are imported here through org.opensearch.commons.replication.
 This helps in making these classes re-usable by other plugins like ism.
 PR details:
 [1] https://github.com/opensearch-project/common-utils/pull/667
 [2] https://github.com/opensearch-project/cross-cluster-replication/pull/1391
 */

class TransportStopIndexReplicationAction @Inject constructor(transportService: TransportService,
                                                              clusterService: ClusterService,
                                                              threadPool: ThreadPool,
                                                              actionFilters: ActionFilters,
                                                              indexNameExpressionResolver:
                                                              IndexNameExpressionResolver,
                                                              val client: Client,
                                                              val replicationMetadataManager: ReplicationMetadataManager,
                                                              val persistentTasksService: PersistentTasksService) :
    TransportClusterManagerNodeAction<StopIndexReplicationRequest, AcknowledgedResponse> (STOP_REPLICATION_ACTION_NAME,
            transportService, clusterService, threadPool, actionFilters, ::StopIndexReplicationRequest,
            indexNameExpressionResolver), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportStopIndexReplicationAction::class.java)
    }

    private val staleArtifactDetector = StaleArtifactDetector(clusterService)

    private val taskCleanupManager = TaskCleanupManager(
        persistentTasksService, clusterService, client, replicationMetadataManager, staleArtifactDetector
    )

    override fun checkBlock(request: StopIndexReplicationRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    @Throws(Exception::class)
    override fun clusterManagerOperation(request: StopIndexReplicationRequest, state: ClusterState,
                                 listener: ActionListener<AcknowledgedResponse>) {
        launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            try {
                log.info("Stopping replication for index: ${request.indexName}")

                validateStateAndCleanupIfNeeded(request.indexName)
                removeIndexBlocks(request.indexName)

                // Close index BEFORE cluster state update (if not restoring)
                val shouldReopenIndex = !isIndexRestoring(request.indexName) && state.routingTable.hasIndex(request.indexName)
                if (shouldReopenIndex) {
                    closeIndex(request.indexName)
                }

                // Get metadata before cleanup so retention lease removal can use it
                val replMetadata = try {
                    replicationMetadataManager.getIndexReplicationMetadata(request.indexName)
                } catch (e: Exception) {
                    log.debug("No replication metadata found for ${request.indexName}, skipping retention lease removal")
                    null
                }

                // CRITICAL: Update cluster state FIRST (removes blocks/settings)
                // This prepares the index for cleanup operations
                updateClusterState(request.indexName)

                // Now cleanup: remove retention leases and unassigned persistent tasks
                // Note: Tasks will cancel themselves when we remove ReplicationStateMetadata below
                val cleanupResult = taskCleanupManager.cleanupAllReplicationTasks(request.indexName, replMetadata)

                // Reopen index AFTER cluster state update (so blocks/settings are removed)
                if (shouldReopenIndex) {
                    reopenIndex(request.indexName)
                }

                // CONDITIONAL metadata deletion based on cleanup result
                // This enables both idempotent retry AND auto-delete functionality
                if (cleanupResult.success) {
                    // Full cleanup succeeded - safe to delete metadata completely
                    try {
                        replicationMetadataManager.deleteIndexReplicationMetadata(request.indexName)
                        log.info("Successfully deleted replication metadata for ${request.indexName}")
                    } catch (e: Exception) {
                        log.debug("Metadata already deleted for ${request.indexName}")
                    }
                } else {
                    // Cleanup had failures - keep metadata store for retry BUT remove ReplicationStateMetadata
                    // to trigger task cancellation for consistency
                    log.warn("Cleanup had failures for ${request.indexName}, keeping metadata store for retry. Failures: ${cleanupResult.failures}")
                    try {
                        replicationMetadataManager.removeReplicationStateOnly(request.indexName)
                        log.info("Removed replication state metadata for ${request.indexName} to trigger task cancellation")
                    } catch (e: Exception) {
                        log.warn("Failed to remove replication state for ${request.indexName}", e)
                    }
                    // Still return success since cluster state was updated and index is stopped from user perspective
                    // Metadata will be cleaned up on next STOP call (idempotent)
                }

                log.info("Successfully stopped replication for ${request.indexName}")
                listener.onResponse(AcknowledgedResponse(true))
            } catch (e: Exception) {
                log.error("Stop replication failed for ${request.indexName}: ${e.stackTraceToString()}")
                listener.onFailure(e)
            }
        }
    }

    /**
     * Validates current state and performs cleanup if needed.
     * Implements idempotent behavior by handling already-stopped state gracefully.
     */
    private suspend fun validateStateAndCleanupIfNeeded(indexName: String) {
        val replicationStateParams = getReplicationStateParamsForIndex(clusterService, indexName)

        if (replicationStateParams == null) {
            handleMissingReplicationState(indexName)
            return
        }

        val currentState = replicationStateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE]

        when (currentState) {
            ReplicationOverallState.STOPPED.name -> {
                log.info("Replication already stopped for $indexName - performing cleanup for consistency")
                // Idempotent: cleanup any remaining artifacts
            }
            ReplicationOverallState.RUNNING.name,
            ReplicationOverallState.FAILED.name,
            ReplicationOverallState.PAUSED.name -> {
                // Valid states for stopping - proceed
            }
            else -> {
                throw IllegalStateException("Unknown replication state: $currentState")
            }
        }
    }

    /**
     * Handles case where no replication state exists but stale artifacts might remain.
     * Implements idempotent behavior while still validating that the index exists.
     */
    private suspend fun handleMissingReplicationState(indexName: String) {
        val artifactReport = staleArtifactDetector.detectStaleArtifacts(indexName)

        if (artifactReport.hasStaleArtifacts) {
            log.info("Found ${artifactReport.artifacts.size} stale artifacts for $indexName, cleaning up")
            try {
                taskCleanupManager.cleanupStaleArtifacts(indexName)
            } catch (e: Exception) {
                log.warn("Stale artifact cleanup failed for $indexName", e)
            }
            // After cleaning up stale artifacts, continue with stop operation
            return
        }
        
        // No replication state and no stale artifacts
        // Check if the index exists at all
        val indexExists = clusterService.state().metadata.hasIndex(indexName)
        
        if (!indexExists) {
            // Index doesn't exist - this is an error (user called STOP on non-existent index)
            throw IllegalArgumentException("No replication in progress for index:$indexName")
        }
        
        // Index exists but is not being replicated
        // This could mean:
        // 1. STOP called on wrong cluster (e.g., leader instead of follower) - OK for auto-delete
        // 2. Replication already stopped (idempotent call) - OK
        // 3. Index was never replicated - OK
        // In all cases, succeed gracefully
        log.info("Index $indexName exists but is not being replicated - nothing to stop")
    }

    /**
     * Removes index blocks in an idempotent manner.
     */
    private suspend fun removeIndexBlocks(indexName: String) {
        try {
            val updateIndexBlockRequest = UpdateIndexBlockRequest(indexName, IndexBlockUpdateType.REMOVE_BLOCK)
            val response = client.suspendExecute(UpdateIndexBlockAction.INSTANCE,updateIndexBlockRequest, injectSecurityContext = true)

            if (!response.isAcknowledged) {
                log.warn("Failed to remove index block on $indexName, continuing with cleanup")
            }
        } catch (e: Exception) {
            log.warn("Exception while removing index block for $indexName", e)
        }
    }

    /**
     * Checks if the index is currently being restored.
     */
    private fun isIndexRestoring(indexName: String): Boolean {
        return clusterService.state()
            .custom<RestoreInProgress>(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)
            .any { entry -> entry.indices().any { it == indexName } }
    }

    /**
     * Closes the index before cluster state update.
     */
    private suspend fun closeIndex(indexName: String) {
        try {
            val updateRequest = UpdateMetadataRequest(
                indexName,
                UpdateMetadataRequest.Type.CLOSE,
                Requests.closeIndexRequest(indexName)
            )
            val closeResponse = client.suspendExecute(
                UpdateMetadataAction.INSTANCE,
                updateRequest,
                injectSecurityContext = true
            )

            if (!closeResponse.isAcknowledged) {
                throw OpenSearchException("Unable to close index: $indexName")
            }
        } catch (e: Exception) {
            log.error("Failed to close index $indexName", e)
            throw e
        }
    }

    /**
     * Reopens the index after cluster state update (blocks/settings removed).
     */
    private suspend fun reopenIndex(indexName: String) {
        try {
            val reopenResponse = client.suspending(
                client.admin().indices()::open,
                injectSecurityContext = true
            )(OpenIndexRequest(indexName))

            if (!reopenResponse.isAcknowledged) {
                throw OpenSearchException("Failed to reopen index: $indexName")
            }
        } catch (e: Exception) {
            log.error("Failed to reopen index $indexName", e)
            throw e
        }
    }

    /**
     * Updates cluster state to remove replication blocks and settings.
     */
    private suspend fun updateClusterState(indexName: String) {
        val response: AcknowledgedResponse = clusterService.waitForClusterStateUpdate("stop_replication") { l ->
            StopReplicationTask(StopIndexReplicationRequest(indexName), l)
        }

        if (!response.isAcknowledged) {
            throw OpenSearchException("Failed to update cluster state for $indexName")
        }
    }

    override fun executor(): String {
        return ThreadPool.Names.SAME
    }

    @Throws(IOException::class)
    override fun read(inp: StreamInput): AcknowledgedResponse {
        return AcknowledgedResponse(inp)
    }

    class StopReplicationTask(val request: StopIndexReplicationRequest, listener: ActionListener<AcknowledgedResponse>) :
        AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

        override fun execute(currentState: ClusterState): ClusterState {
            val newState = ClusterState.builder(currentState)
            // remove index block
            if (currentState.blocks.hasIndexBlock(request.indexName, INDEX_REPLICATION_BLOCK)) {
                val newBlocks = ClusterBlocks.builder().blocks(currentState.blocks)
                    .removeIndexBlock(request.indexName, INDEX_REPLICATION_BLOCK)
                newState.blocks(newBlocks)
            }

            val mdBuilder = Metadata.builder(currentState.metadata)
            // remove replicated index setting
            val currentIndexMetadata = currentState.metadata.index(request.indexName)
            if (currentIndexMetadata != null &&
                    currentIndexMetadata.settings[REPLICATED_INDEX_SETTING.key] != null) {
                val newIndexMetadata = IndexMetadata.builder(currentIndexMetadata)
                        .settings(Settings.builder().put(currentIndexMetadata.settings).putNull(REPLICATED_INDEX_SETTING.key))
                        .settingsVersion(1 + currentIndexMetadata.settingsVersion)
                mdBuilder.put(newIndexMetadata)
            }
            newState.metadata(mdBuilder)
            
            return newState.build()
        }

        override fun newResponse(acknowledged: Boolean) = AcknowledgedResponse(acknowledged)
    }
}
