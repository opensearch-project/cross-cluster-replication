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

import org.opensearch.replication.ReplicationException
import org.opensearch.replication.action.replicationstatedetails.UpdateReplicationStateAction
import org.opensearch.replication.action.replicationstatedetails.UpdateReplicationStateDetailsRequest
import org.opensearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import org.opensearch.replication.metadata.store.*
import org.opensearch.replication.repository.RemoteClusterRepository
import org.opensearch.replication.util.overrideFgacRole
import org.opensearch.replication.util.suspendExecute
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import java.util.concurrent.CopyOnWriteArrayList
import org.opensearch.commons.authuser.User
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.OpenSearchException
import org.opensearch.ResourceNotFoundException
import org.opensearch.action.DocWriteResponse
import org.opensearch.transport.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Singleton
import org.opensearch.common.settings.Settings
import org.opensearch.commons.utils.OpenForTesting

@Singleton
@OpenForTesting
//ToDo : Debug why OpenForTesting is not working properly
open class ReplicationMetadataManager constructor(private val clusterService: ClusterService,
                                             private val client: Client,
                                             private val replicaionMetadataStore: ReplicationMetadataStore) {

    companion object {
        private val log = LogManager.getLogger(ReplicationMetadataManager::class.java)
        const val CUSTOMER_INITIATED_ACTION = "User initiated"
    }

    suspend fun addIndexReplicationMetadata(followerIndex: String,
                                            connectionName: String,
                                            leaderIndex: String,
                                            overallState: ReplicationOverallState,
                                            user: User?,
                                            follower_cluster_role: String?,
                                            leader_cluster_role: String?,
                                            settings: Settings) {
        val replicationMetadata = ReplicationMetadata(connectionName,
                ReplicationStoreMetadataType.INDEX.name, overallState.name, CUSTOMER_INITIATED_ACTION,
                ReplicationContext(followerIndex, user?.overrideFgacRole(follower_cluster_role)),
                ReplicationContext(leaderIndex, user?.overrideFgacRole(leader_cluster_role)), settings)
        addMetadata(AddReplicationMetadataRequest(replicationMetadata))
        updateReplicationState(followerIndex, overallState, connectionName)
    }

    /**
     * Adds index replication metadata for multiple follower indices in as few round-trips as possible:
     * one bulk write to the replication metadata store, followed by per-index replication-state updates
     * (which are coalesced into a single cluster-state publish by UpdateReplicationStateDetailsTaskExecutor).
     * Used by the bulk Start API. Assumes follower index name == leader index name (as resolved by bulk start).
     * Returns the set of follower indices for which BOTH the doc write and the state update succeeded.
     */
    suspend fun addIndexReplicationMetadata(followerIndices: List<String>,
                                            connectionName: String,
                                            overallState: ReplicationOverallState,
                                            user: User?,
                                            follower_cluster_role: String?,
                                            leader_cluster_role: String?,
                                            settings: Settings): Set<String> {
        if (followerIndices.isEmpty()) return emptySet()

        val addReqs = followerIndices.map { index ->
            AddReplicationMetadataRequest(
                ReplicationMetadata(connectionName,
                    ReplicationStoreMetadataType.INDEX.name, overallState.name, CUSTOMER_INITIATED_ACTION,
                    ReplicationContext(index, user?.overrideFgacRole(follower_cluster_role)),
                    ReplicationContext(index, user?.overrideFgacRole(leader_cluster_role)), settings))
        }
        @Suppress("UNCHECKED_CAST")

        val written = executeAndWrapExceptionIfAny({
            replicaionMetadataStore.addMetadata(addReqs)
        }, log, "Error adding replication metadata") as Set<String>

        val stateUpdated = updateReplicationStateConcurrently(written, overallState, connectionName)
        // Clean up metadata docs for indices where state update failed
        val orphaned = written - stateUpdated
        if (orphaned.isNotEmpty()) {
            try { replicaionMetadataStore.deleteMetadata(ReplicationStoreMetadataType.INDEX.name, orphaned.toList()) }
            catch (e: Exception) { log.error("Failed to clean up orphaned metadata for $orphaned: ${e.message}") }
        }
        return stateUpdated
    }

    suspend fun addAutofollowMetadata(patternName: String, connectionName: String, pattern: String,
                                      overallState: ReplicationOverallState, user: User?,
                                      follower_cluster_role: String?, leader_cluster_role: String?, settings: Settings,
                                      followerIndexPattern: String? = null) {
        val replicationMetadata = ReplicationMetadata(connectionName,
                ReplicationStoreMetadataType.AUTO_FOLLOW.name, overallState.name, CUSTOMER_INITIATED_ACTION,
                ReplicationContext(patternName, user?.overrideFgacRole(follower_cluster_role)),
                ReplicationContext(pattern, user?.overrideFgacRole(leader_cluster_role)), settings, followerIndexPattern)
        addMetadata(AddReplicationMetadataRequest(replicationMetadata))
    }

    private suspend fun addMetadata(metadataReq: AddReplicationMetadataRequest) {
        executeAndWrapExceptionIfAny({
            val response = replicaionMetadataStore.addMetadata(metadataReq)
            if(response.result != DocWriteResponse.Result.CREATED &&
                    response.result != DocWriteResponse.Result.UPDATED) {
                log.error("Encountered error with result - ${response.result}, while adding metadata")
                throw ReplicationException("Error adding replication metadata")
            }
        }, log, "Error adding replication metadata")
    }

    suspend fun updateIndexReplicationState(followerIndex: String,
                                            overallState: ReplicationOverallState,
                                            reason: String = CUSTOMER_INITIATED_ACTION) {
        val getReq = GetReplicationMetadataRequest(ReplicationStoreMetadataType.INDEX.name, null, followerIndex)
        val getRes = replicaionMetadataStore.getMetadata(getReq, false)
        val updatedMetadata = getRes.replicationMetadata
        updatedMetadata.overallState = overallState.name
        updatedMetadata.reason = reason
        updateMetadata(UpdateReplicationMetadataRequest(updatedMetadata, getRes.seqNo, getRes.primaryTerm))
        updateReplicationState(followerIndex, overallState)
    }

    /**
     * Updates the overall replication state for multiple follower indices in as few round-trips as possible:
     * one MultiGet (for current docs + versions), one bulk update to the store (optimistic-locked), followed by
     * per-index replication-state updates (coalesced into a single cluster-state publish by the state executor).
     * Used by the bulk Pause/Resume APIs. Returns the set of indices updated successfully end-to-end.
     */
    suspend fun updateIndexReplicationState(followerIndices: List<String>,
                                            overallState: ReplicationOverallState,
                                            reason: String = CUSTOMER_INITIATED_ACTION): Set<String> {
        if (followerIndices.isEmpty()) return emptySet()

        @Suppress("UNCHECKED_CAST")
        val current = executeAndWrapExceptionIfAny({
            replicaionMetadataStore.getMetadataWithVersion(ReplicationStoreMetadataType.INDEX.name, followerIndices)
        }, log, "Failed to fetch replication metadata") as Map<String, GetReplicationMetadataResponse>

        val updateReqs = ArrayList<UpdateReplicationMetadataRequest>()
        for (index in followerIndices) {
            val getRes = current[index] ?: continue
            val updatedMetadata = getRes.replicationMetadata
            updatedMetadata.overallState = overallState.name
            updatedMetadata.reason = reason
            updateReqs.add(UpdateReplicationMetadataRequest(updatedMetadata, getRes.seqNo, getRes.primaryTerm))
        }
        if (updateReqs.isEmpty()) return emptySet()

        @Suppress("UNCHECKED_CAST")
        val written = executeAndWrapExceptionIfAny({
            replicaionMetadataStore.updateMetadata(updateReqs)
        }, log, "Error updating replication metadata") as Set<String>

        return updateReplicationStateConcurrently(written, overallState)
    }

    suspend fun updateAutofollowMetadata(patternName: String,
                                         connectionName: String,
                                         pattern: String,
                                         reason: String = CUSTOMER_INITIATED_ACTION) {
        val getReq = GetReplicationMetadataRequest(ReplicationStoreMetadataType.AUTO_FOLLOW.name, connectionName, patternName)
        val getRes = replicaionMetadataStore.getMetadata(getReq, false)
        val updatedMetadata = getRes.replicationMetadata
        updatedMetadata.leaderContext.resource = pattern
        updatedMetadata.reason = reason
        updateMetadata(UpdateReplicationMetadataRequest(updatedMetadata, getRes.seqNo, getRes.primaryTerm))
    }

    private suspend fun updateMetadata(updateReq: UpdateReplicationMetadataRequest) {
        executeAndWrapExceptionIfAny({
            val response = replicaionMetadataStore.updateMetadata(updateReq)
            if(response.result != DocWriteResponse.Result.CREATED &&
                    response.result != DocWriteResponse.Result.UPDATED) {
                log.error("Encountered error with result - ${response.result}, while updating metadata")
                throw org.opensearch.replication.ReplicationException("Error updating replication metadata")
            }
        }, log, "Error updating replication metadata")
    }

    suspend fun updateSettings(followerIndex: String, settings: Settings) {
        val getReq = GetReplicationMetadataRequest(ReplicationStoreMetadataType.INDEX.name, null, followerIndex)
        val getRes = replicaionMetadataStore.getMetadata(getReq, false)
        val metadata = getRes.replicationMetadata
        metadata.settings = settings
        updateMetadata(UpdateReplicationMetadataRequest(metadata, getRes.seqNo, getRes.primaryTerm))
    }

    suspend fun deleteIndexReplicationMetadata(followerIndex: String) {
        val delReq = DeleteReplicationMetadataRequest(ReplicationStoreMetadataType.INDEX.name, null, followerIndex)
        deleteMetadata(delReq)
        updateReplicationState(followerIndex, ReplicationOverallState.STOPPED)
    }

    /**
     * Deletes replication metadata for multiple indices in ONE Bulk call.
     * Returns set of indices that were successfully deleted.
     * Note: updateReplicationState(STOPPED) is called per-index since it's a cluster state update.
     */
    suspend fun deleteIndexReplicationMetadata(followerIndices: List<String>): Set<String> {
        val deleted = replicaionMetadataStore.deleteMetadata(ReplicationStoreMetadataType.INDEX.name, followerIndices)
        updateReplicationStateConcurrently(deleted, ReplicationOverallState.STOPPED)
        return deleted
    }

    suspend fun deleteAutofollowMetadata(patternName: String,
                                         connectionName: String) {
        val delReq = DeleteReplicationMetadataRequest(ReplicationStoreMetadataType.AUTO_FOLLOW.name, connectionName, patternName)
        deleteMetadata(delReq)
    }

    private suspend fun deleteMetadata(deleteReq: DeleteReplicationMetadataRequest) {
        executeAndWrapExceptionIfAny({
            val delRes = replicaionMetadataStore.deleteMetadata(deleteReq)
            if(delRes.result == DocWriteResponse.Result.NOT_FOUND) {
                throw ResourceNotFoundException("Metadata for ${deleteReq.resourceName} doesn't exist")
            }
            if(delRes.result != DocWriteResponse.Result.DELETED) {
                log.error("Encountered error with result - ${delRes.result}, while deleting metadata")
                throw ReplicationException("Error deleting replication metadata")
            }
        }, log, "Error deleting replication metadata")
    }

    suspend fun getIndexReplicationMetadata(followerIndex: String, fetch_from_primary: Boolean = false): ReplicationMetadata {
        val getReq = GetReplicationMetadataRequest(ReplicationStoreMetadataType.INDEX.name, null, followerIndex)
        return executeAndWrapExceptionIfAny({
            replicaionMetadataStore.getMetadata(getReq, fetch_from_primary).replicationMetadata
        }, log, "Failed to fetch replication metadata") as ReplicationMetadata
    }

    /**
     * Fetches replication metadata for multiple follower indices in ONE call for bulk API
     * Returns a map of follower index name -> ReplicationMetadata. Missing indices are omitted.
     */
    suspend fun getIndexReplicationMetadata(followerIndices: List<String>): Map<String, ReplicationMetadata> {
        return replicaionMetadataStore.getMetadata(ReplicationStoreMetadataType.INDEX.name, followerIndices)
    }

    fun getIndexReplicationMetadata(followerIndex: String,
                                    connectionName: String?,
                                    fetch_from_primary: Boolean = false,
                                    timeout: Long = RemoteClusterRepository.REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC): ReplicationMetadata {
        val getReq = GetReplicationMetadataRequest(ReplicationStoreMetadataType.INDEX.name, connectionName, followerIndex)
        try {
            return replicaionMetadataStore.getMetadata(getReq, fetch_from_primary, timeout).replicationMetadata
        } catch (e: ResourceNotFoundException) {
            log.error("Encountered exception - ", e)
            throw e
        } catch (e: Exception) {
            log.error("Failed to fetch replication metadata", e)
            throw ReplicationException("Failed to fetch replication metadata")
        }
    }

    suspend fun getAutofollowMetadata(patternName: String,
                                      connectionName: String,
                                      fetch_from_primary: Boolean = false): ReplicationMetadata {
        val getReq = GetReplicationMetadataRequest(ReplicationStoreMetadataType.AUTO_FOLLOW.name, connectionName, patternName)
        return executeAndWrapExceptionIfAny({
            replicaionMetadataStore.getMetadata(getReq, fetch_from_primary).replicationMetadata
        }, log, "Failed to fetch replication metadata") as ReplicationMetadata
    }

    /**
     * Submits per-index replication-state updates concurrently so they are coalesced
     * into a single cluster-state publish by UpdateReplicationStateDetailsTaskExecutor.
     * Used by bulk APIs only. Returns the set of indices that succeeded.
     */
    private suspend fun updateReplicationStateConcurrently(
        indices: Collection<String>,
        overallState: ReplicationOverallState,
        connectionName: String? = null
    ): Set<String> {
        if (indices.isEmpty()) return emptySet()
        val succeeded = CopyOnWriteArrayList<String>()
        coroutineScope {
            indices.map { index -> async {
                try {
                    updateReplicationState(index, overallState, connectionName)
                    succeeded.add(index)
                } catch (e: Exception) {
                    log.error("Failed to update replication state for $index: ${e.message}")
                }
            }}.awaitAll()
        }
        return succeeded.toSet()
    }

    private suspend fun updateReplicationState(indexName: String, overallState: ReplicationOverallState, connectionName: String? = null) {
        val replicationStateParamMap = HashMap<String, String>()
        replicationStateParamMap[REPLICATION_LAST_KNOWN_OVERALL_STATE] = overallState.name
        if (connectionName != null) replicationStateParamMap["connection_name"] = connectionName
        var updateType = UpdateReplicationStateDetailsRequest.UpdateType.ADD
        if(overallState == ReplicationOverallState.STOPPED) {
            updateType = UpdateReplicationStateDetailsRequest.UpdateType.REMOVE
        }
        val updateReplicationStateDetailsRequest = UpdateReplicationStateDetailsRequest(indexName, replicationStateParamMap,
                updateType)
        executeAndWrapExceptionIfAny({
            client.suspendExecute(UpdateReplicationStateAction.INSTANCE, updateReplicationStateDetailsRequest, defaultContext = true)
        }, log, "Error updating replicaiton metadata")
    }

    private suspend fun executeAndWrapExceptionIfAny(tryBlock: suspend () -> Any?, log: Logger, errorMessage: String): Any? {
        try {
            return tryBlock()
        } catch (e: OpenSearchException) {
            log.error("Encountered exception - ", e)
            throw e
        } catch (e: Exception) {
            log.error("Encountered exception - ", e)
            throw ReplicationException(errorMessage)
        }
    }
}
