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
        updateReplicationState(followerIndex, overallState)
    }

    suspend fun addAutofollowMetadata(patternName: String, connectionName: String, pattern: String,
                                      overallState: ReplicationOverallState, user: User?,
                                      follower_cluster_role: String?, leader_cluster_role: String?,
                                      settings: Settings,
                                      // Point 1 & 3 & 5: Checkpoint configuration forwarded from the autofollow request
                                      checkpointPersistenceEnabled: Boolean = true,
                                      checkpointRetentionPeriod: String = "24h",
                                      roleTransitionResumeMode: String = "CHECKPOINT") {
        val replicationMetadata = ReplicationMetadata(connectionName,
                ReplicationStoreMetadataType.AUTO_FOLLOW.name, overallState.name, CUSTOMER_INITIATED_ACTION,
                ReplicationContext(patternName, user?.overrideFgacRole(follower_cluster_role)),
                ReplicationContext(pattern, user?.overrideFgacRole(leader_cluster_role)),
                settings,
                checkpointPersistenceEnabled,
                checkpointRetentionPeriod,
                roleTransitionResumeMode)
        addMetadata(AddReplicationMetadataRequest(replicationMetadata))
    }

    /**
     * Point 2: Update persisted leader and follower checkpoint sequences for a given autofollow pattern.
     * Called periodically during active replication so that a role transition can resume from the
     * last known position rather than performing a full re-sync.
     */
    suspend fun updateCheckpointSequences(patternName: String, connectionName: String,
                                          leaderSequenceNumber: Long, followerAppliedSequence: Long) {
        executeAndWrapExceptionIfAny({
            val getReq = GetReplicationMetadataRequest(ReplicationStoreMetadataType.AUTO_FOLLOW.name, connectionName, patternName)
            val getRes = replicaionMetadataStore.getMetadata(getReq, false)
            val metadata = getRes.replicationMetadata
            if (metadata.checkpointPersistenceEnabled) {
                metadata.leaderSequenceNumber = leaderSequenceNumber
                metadata.followerAppliedSequence = followerAppliedSequence
                replicaionMetadataStore.updateMetadata(UpdateReplicationMetadataRequest(metadata))
                log.debug("Updated checkpoint sequences for $connectionName:$patternName — " +
                        "leader=$leaderSequenceNumber follower=$followerAppliedSequence")
            }
        }, log, "Error updating checkpoint sequences for $patternName")
    }

    /**
     * Update persisted leader and follower checkpoint sequences for a specific replicated index (INDEX type).
     * Called periodically from ShardReplicationTask (co-located with the shard) so role transitions
     * can resume from the last known position.
     * No optimistic locking — multiple shard tasks for the same index may call this concurrently;
     * last write wins, which is acceptable since all writes carry approximately correct values.
     */
    suspend fun updateIndexCheckpointSequences(followerIndex: String,
                                               leaderSequenceNumber: Long,
                                               followerAppliedSequence: Long) {
        executeAndWrapExceptionIfAny({
            val getReq = GetReplicationMetadataRequest(ReplicationStoreMetadataType.INDEX.name, null, followerIndex)
            val getRes = replicaionMetadataStore.getMetadata(getReq, false)
            val metadata = getRes.replicationMetadata
            if (metadata.checkpointPersistenceEnabled) {
                metadata.leaderSequenceNumber = leaderSequenceNumber
                metadata.followerAppliedSequence = followerAppliedSequence
                // No seqNo/primaryTerm → no optimistic locking → concurrent shard-task writes are safe
                replicaionMetadataStore.updateMetadata(UpdateReplicationMetadataRequest(metadata))
                log.debug("Persisted checkpoint for index $followerIndex — " +
                        "leader=$leaderSequenceNumber follower=$followerAppliedSequence")
            }
        }, log, "Error persisting checkpoint sequences for index $followerIndex")
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

    /**
     * Called when replication is explicitly stopped. Instead of deleting the INDEX metadata document,
     * we mark it as STOPPED while preserving the checkpoint fields (leaderSequenceNumber,
     * followerAppliedSequence). This allows a subsequent role transition to resume from the last
     * known checkpoint rather than performing a full re-sync.
     *
     * When replication is re-started fresh (not a role transition), addIndexReplicationMetadata()
     * overwrites this STOPPED document with a new RUNNING state, so there is no data leak.
     */
    suspend fun deleteIndexReplicationMetadata(followerIndex: String) {
        try {
            val getReq = GetReplicationMetadataRequest(ReplicationStoreMetadataType.INDEX.name, null, followerIndex)
            val getRes = replicaionMetadataStore.getMetadata(getReq, false)
            val metadata = getRes.replicationMetadata
            if (metadata.leaderSequenceNumber > ReplicationMetadata.UNASSIGNED_SEQ_NO) {
                // Checkpoint exists — preserve it by transitioning to STOPPED state
                metadata.overallState = ReplicationOverallState.STOPPED.name
                metadata.reason = CUSTOMER_INITIATED_ACTION
                replicaionMetadataStore.updateMetadata(
                    UpdateReplicationMetadataRequest(metadata, getRes.seqNo, getRes.primaryTerm))
                log.info("Preserved checkpoint for $followerIndex on stop — " +
                        "leaderSeqNo=${metadata.leaderSequenceNumber}, followerSeqNo=${metadata.followerAppliedSequence}. " +
                        "Role transition can resume from this checkpoint.")
            } else {
                // No checkpoint recorded — safe to delete (nothing useful to preserve)
                val delReq = DeleteReplicationMetadataRequest(ReplicationStoreMetadataType.INDEX.name, null, followerIndex)
                deleteMetadata(delReq)
                log.debug("Deleted INDEX metadata for $followerIndex (no checkpoint to preserve)")
            }
        } catch (e: ResourceNotFoundException) {
            log.debug("INDEX metadata for $followerIndex not found during stop — already cleaned up")
        } catch (e: Exception) {
            // Fall back to delete so stop replication doesn't get stuck
            log.warn("Failed to preserve checkpoint for $followerIndex, falling back to delete: ${e.message}")
            try {
                val delReq = DeleteReplicationMetadataRequest(ReplicationStoreMetadataType.INDEX.name, null, followerIndex)
                deleteMetadata(delReq)
            } catch (ex: Exception) {
                log.error("Failed to clean up metadata for $followerIndex: ${ex.message}")
            }
        }
        updateReplicationState(followerIndex, ReplicationOverallState.STOPPED)
    }

    suspend fun deleteAutofollowMetadata(patternName: String,
                                         connectionName: String) {
        val delReq = DeleteReplicationMetadataRequest(ReplicationStoreMetadataType.AUTO_FOLLOW.name, connectionName, patternName)
        deleteMetadata(delReq)
    }

    private suspend fun deleteMetadata(deleteReq: DeleteReplicationMetadataRequest) {
        executeAndWrapExceptionIfAny({
            val delRes = replicaionMetadataStore.deleteMetadata(deleteReq)
            if(delRes.result != DocWriteResponse.Result.DELETED && delRes.result != DocWriteResponse.Result.NOT_FOUND) {
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

    private suspend fun updateReplicationState(indexName: String, overallState: ReplicationOverallState) {
        val replicationStateParamMap = HashMap<String, String>()
        replicationStateParamMap[REPLICATION_LAST_KNOWN_OVERALL_STATE] = overallState.name
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
