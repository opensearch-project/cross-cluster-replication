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

package org.opensearch.replication.action.pause

import org.opensearch.replication.metadata.*
import org.opensearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import org.opensearch.replication.metadata.state.getReplicationStateParamsForIndex
import org.opensearch.replication.task.cleanup.TaskCleanupManager
import org.opensearch.replication.task.cleanup.StaleArtifactDetector
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.stackTraceToString
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchException
import org.opensearch.core.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction
import org.opensearch.transport.client.Client
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.RestoreInProgress
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.persistent.PersistentTasksService
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import java.io.IOException

class TransportPauseIndexReplicationAction @Inject constructor(transportService: TransportService,
                                                               clusterService: ClusterService,
                                                               threadPool: ThreadPool,
                                                               actionFilters: ActionFilters,
                                                               indexNameExpressionResolver:
                                                               IndexNameExpressionResolver,
                                                               val client: Client,
                                                               val replicationMetadataManager: ReplicationMetadataManager,
                                                               val persistentTasksService: PersistentTasksService) :
    TransportClusterManagerNodeAction<PauseIndexReplicationRequest, AcknowledgedResponse> (PauseIndexReplicationAction.NAME,
            transportService, clusterService, threadPool, actionFilters, ::PauseIndexReplicationRequest,
            indexNameExpressionResolver), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportPauseIndexReplicationAction::class.java)
    }

    private val staleArtifactDetector = StaleArtifactDetector(clusterService)
    
    private val taskCleanupManager = TaskCleanupManager(
        persistentTasksService, clusterService, client, replicationMetadataManager, staleArtifactDetector
    )

    override fun checkBlock(request: PauseIndexReplicationRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    @Throws(Exception::class)
    override fun clusterManagerOperation(request: PauseIndexReplicationRequest, state: ClusterState,
                                 listener: ActionListener<AcknowledgedResponse>) {
        launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            try {
                log.info("Pausing replication for index: ${request.indexName}")

                // Check current state for idempotency logic
                val replicationStateParams = getReplicationStateParamsForIndex(clusterService, request.indexName)
                val currentState = replicationStateParams?.get(REPLICATION_LAST_KNOWN_OVERALL_STATE)
                val isAlreadyPaused = currentState == ReplicationOverallState.PAUSED.name

                validateStateAndCleanupIfNeeded(request.indexName)
                checkNotRestoring(request.indexName)

                // CRITICAL: Update state to PAUSED FIRST to trigger task cancellation via ClusterStateListener
                // This allows tasks to cancel themselves and clean up their stats from FollowerClusterStats
                // Only update state if not already PAUSED (idempotent optimization)
                if (!isAlreadyPaused) {
                    replicationMetadataManager.updateIndexReplicationState(
                        request.indexName,
                        ReplicationOverallState.PAUSED,
                        request.reason
                    )
                } else {
                    log.info("State already PAUSED for ${request.indexName}, skipping state update")
                }

                // Now cleanup: remove unassigned persistent tasks
                // Tasks should have cancelled themselves by now via ClusterStateListener
                // Note: We don't remove retention leases during PAUSE (they're kept for RESUME)
                val cleanupResult = taskCleanupManager.suspendReplicationTasks(request.indexName)

                log.info("Successfully paused replication for ${request.indexName}")
                listener.onResponse(AcknowledgedResponse(true))
            } catch (e: Exception) {
                log.error("Pause replication failed for ${request.indexName}: ${e.stackTraceToString()}")
                listener.onFailure(e)
            }
        }
    }

    /**
     * Validates current state and performs cleanup if needed.
     * Implements idempotent behavior by handling already-paused state gracefully.
     */
    private suspend fun validateStateAndCleanupIfNeeded(indexName: String) {
        val replicationStateParams = getReplicationStateParamsForIndex(clusterService, indexName)

        if (replicationStateParams == null) {
            handleMissingReplicationState(indexName)
            return
        }

        val currentState = replicationStateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE]

        when (currentState) {
            ReplicationOverallState.PAUSED.name -> {
                log.info("Index $indexName already paused - will verify cleanup in main flow")
                // Don't cleanup here to avoid double cleanup - main flow will handle it
            }
            ReplicationOverallState.RUNNING.name -> {
                // Normal case - proceed with pause
            }
            else -> {
                throw IllegalStateException("Cannot pause when in $currentState state")
            }
        }
    }

    /**
     * Handles case where no replication state exists but stale artifacts might remain.
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
        }

        throw IllegalArgumentException("No replication in progress for index:$indexName")
    }

    /**
     * Checks if the index is currently being restored and throws if so.
     */
    private fun checkNotRestoring(indexName: String) {
        val restoring = clusterService.state()
            .custom<RestoreInProgress>(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)
            .any { entry -> entry.indices().any { it == indexName } }

        if (restoring) {
            throw OpenSearchException(
                "Index is in restore phase currently for index: $indexName"
            )
        }
    }

    override fun executor(): String {
        return ThreadPool.Names.SAME
    }

    @Throws(IOException::class)
    override fun read(inp: StreamInput): AcknowledgedResponse {
        return AcknowledgedResponse(inp)
    }
}
