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
import org.opensearch.action.DocWriteResponse
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Singleton
import org.opensearch.common.settings.Settings

@Singleton
class ReplicationMetadataManager constructor(private val clusterService: ClusterService,
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
                                      follower_cluster_role: String?, leader_cluster_role: String?, settings: Settings) {
        val replicationMetadata = ReplicationMetadata(connectionName,
                ReplicationStoreMetadataType.AUTO_FOLLOW.name, overallState.name, CUSTOMER_INITIATED_ACTION,
                ReplicationContext(patternName, user?.overrideFgacRole(follower_cluster_role)),
                ReplicationContext(pattern, user?.overrideFgacRole(leader_cluster_role)), settings)
        addMetadata(AddReplicationMetadataRequest(replicationMetadata))
    }

    private suspend fun addMetadata(metadataReq: AddReplicationMetadataRequest) {
        val response = replicaionMetadataStore.addMetadata(metadataReq)
        if(response.result != DocWriteResponse.Result.CREATED &&
                response.result != DocWriteResponse.Result.UPDATED) {
            log.error("Encountered error with result - ${response.result}, while adding metadata")
            throw org.opensearch.replication.ReplicationException("Error adding replication metadata")
        }
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
        val response = replicaionMetadataStore.updateMetadata(updateReq)
        if(response.result != DocWriteResponse.Result.CREATED &&
                response.result != DocWriteResponse.Result.UPDATED) {
            log.error("Encountered error with result - ${response.result}, while updating metadata")
            throw org.opensearch.replication.ReplicationException("Error updating replication metadata")
        }
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

    suspend fun deleteAutofollowMetadata(patternName: String,
                                         connectionName: String) {
        val delReq = DeleteReplicationMetadataRequest(ReplicationStoreMetadataType.AUTO_FOLLOW.name, connectionName, patternName)
        deleteMetadata(delReq)
    }

    private suspend fun deleteMetadata(deleteReq: DeleteReplicationMetadataRequest) {
        val delRes = replicaionMetadataStore.deleteMetadata(deleteReq)
        if(delRes.result != DocWriteResponse.Result.DELETED && delRes.result != DocWriteResponse.Result.NOT_FOUND) {
            log.error("Encountered error with result - ${delRes.result}, while deleting metadata")
            throw org.opensearch.replication.ReplicationException("Error deleting replication metadata")
        }
    }

    suspend fun getIndexReplicationMetadata(followerIndex: String, fetch_from_primary: Boolean = false): ReplicationMetadata {
        val getReq = GetReplicationMetadataRequest(ReplicationStoreMetadataType.INDEX.name, null, followerIndex)
        return replicaionMetadataStore.getMetadata(getReq, fetch_from_primary).replicationMetadata
    }

    fun getIndexReplicationMetadata(followerIndex: String,
                                    connectionName: String?,
                                    fetch_from_primary: Boolean = false,
                                    timeout: Long = RemoteClusterRepository.REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC): ReplicationMetadata {
        val getReq = GetReplicationMetadataRequest(ReplicationStoreMetadataType.INDEX.name, connectionName, followerIndex)
        return replicaionMetadataStore.getMetadata(getReq, fetch_from_primary, timeout).replicationMetadata
    }

    suspend fun getAutofollowMetadata(patternName: String,
                                      connectionName: String,
                                      fetch_from_primary: Boolean = false): ReplicationMetadata {
        val getReq = GetReplicationMetadataRequest(ReplicationStoreMetadataType.AUTO_FOLLOW.name, connectionName, patternName)
        return replicaionMetadataStore.getMetadata(getReq, fetch_from_primary).replicationMetadata
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
        client.suspendExecute(UpdateReplicationStateAction.INSTANCE, updateReplicationStateDetailsRequest, defaultContext = true)
    }
}
