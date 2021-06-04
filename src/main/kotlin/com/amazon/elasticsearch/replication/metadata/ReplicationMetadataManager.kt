package com.amazon.elasticsearch.replication.metadata

import com.amazon.elasticsearch.replication.ReplicationException
import com.amazon.elasticsearch.replication.action.replicationstatedetails.UpdateReplicationStateDetailsRequest
import com.amazon.elasticsearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import com.amazon.elasticsearch.replication.metadata.store.AddReplicationMetadataRequest
import com.amazon.elasticsearch.replication.metadata.store.DeleteReplicationMetadataRequest
import com.amazon.elasticsearch.replication.metadata.store.GetReplicationMetadataRequest
import com.amazon.elasticsearch.replication.metadata.store.ReplicationContext
import com.amazon.elasticsearch.replication.metadata.store.ReplicationMetadata
import com.amazon.elasticsearch.replication.metadata.store.ReplicationMetadataStore
import com.amazon.elasticsearch.replication.metadata.store.ReplicationStoreMetadataType
import com.amazon.elasticsearch.replication.metadata.store.UpdateReplicationMetadataRequest
import com.amazon.elasticsearch.replication.repository.RemoteClusterRepository
import com.amazon.elasticsearch.replication.util.overrideFgacRole
import com.amazon.elasticsearch.replication.util.submitClusterStateUpdateTask
import com.amazon.opendistroforelasticsearch.commons.authuser.User
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.DocWriteResponse
import org.elasticsearch.action.support.master.AcknowledgedRequest
import org.elasticsearch.cluster.ClusterStateTaskExecutor
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Singleton

@Singleton
class ReplicationMetadataManager constructor(private val clusterService: ClusterService,
                                             private val replicaionMetadataStore: ReplicationMetadataStore) {

    companion object {
        private val log = LogManager.getLogger(ReplicationMetadataManager::class.java)
    }

    suspend fun addIndexReplicationMetadata(followerIndex: String,
                                            connectionName: String,
                                            leaderIndex: String,
                                            overallState: ReplicationOverallState,
                                            user: User?,
                                            follower_fgac_role: String?,
                                            leader_fgac_role: String?) {
        val replicationMetadata = ReplicationMetadata(connectionName,
                ReplicationStoreMetadataType.INDEX.name, overallState.name,
                ReplicationContext(followerIndex, user?.overrideFgacRole(follower_fgac_role)),
                ReplicationContext(leaderIndex, user?.overrideFgacRole(leader_fgac_role)))
        addMetadata(AddReplicationMetadataRequest(replicationMetadata))
        updateReplicationState(followerIndex, overallState)
    }

    suspend fun addAutofollowMetadata(patternName: String, connectionName: String, pattern: String,
                                      overallState: ReplicationOverallState, user: User?,
                                      follower_fgac_role: String?, leader_fgac_role: String?) {
        val replicationMetadata = ReplicationMetadata(connectionName,
                ReplicationStoreMetadataType.AUTO_FOLLOW.name, overallState.name,
                ReplicationContext(patternName, user?.overrideFgacRole(follower_fgac_role)),
                ReplicationContext(pattern, user?.overrideFgacRole(leader_fgac_role)))
        addMetadata(AddReplicationMetadataRequest(replicationMetadata))
    }

    private suspend fun addMetadata(metadataReq: AddReplicationMetadataRequest) {
        val response = replicaionMetadataStore.addMetadata(metadataReq)
        if(response.result != DocWriteResponse.Result.CREATED &&
                response.result != DocWriteResponse.Result.UPDATED) {
            log.error("Encountered error with result - ${response.result}, while adding metadata")
            throw ReplicationException("Error adding replication metadata")
        }
    }

    suspend fun updateIndexReplicationState(followerIndex: String,
                                            connectionName: String,
                                            overallState: ReplicationOverallState) {
        val getReq = GetReplicationMetadataRequest(ReplicationStoreMetadataType.INDEX.name, connectionName, followerIndex)
        val getRes = replicaionMetadataStore.getMetadata(getReq)
        val updatedMetadata = getRes.replicationMetadata
        updatedMetadata.overallState = overallState.name
        updateMetadata(UpdateReplicationMetadataRequest(updatedMetadata, getRes.seqNo, getRes.primaryTerm))
        updateReplicationState(followerIndex, overallState)
    }

    suspend fun updateAutofollowMetadata(patternName: String,
                                         connectionName: String,
                                         pattern: String) {
        val getReq = GetReplicationMetadataRequest(ReplicationStoreMetadataType.AUTO_FOLLOW.name, connectionName, patternName)
        val getRes = replicaionMetadataStore.getMetadata(getReq)
        val updatedMetadata = getRes.replicationMetadata
        updatedMetadata.leaderContext.resource = pattern
        updateMetadata(UpdateReplicationMetadataRequest(updatedMetadata, getRes.seqNo, getRes.primaryTerm))
    }

    private suspend fun updateMetadata(updateReq: UpdateReplicationMetadataRequest) {
        val response = replicaionMetadataStore.updateMetadata(updateReq)
        if(response.result != DocWriteResponse.Result.CREATED &&
                response.result != DocWriteResponse.Result.UPDATED) {
            throw ReplicationException("Error updating replication metadata")
        }
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
            throw ReplicationException("Error deleting replication metadata")
        }
    }

    suspend fun getIndexReplicationMetadata(followerIndex: String,
                                            connectionName: String?): ReplicationMetadata {
        val getReq = GetReplicationMetadataRequest(ReplicationStoreMetadataType.INDEX.name, connectionName, followerIndex)
        return replicaionMetadataStore.getMetadata(getReq).replicationMetadata
    }

    fun getIndexReplicationMetadata(followerIndex: String,
                                    connectionName: String?,
                                    timeout: Long = RemoteClusterRepository.REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC): ReplicationMetadata {
        val getReq = GetReplicationMetadataRequest(ReplicationStoreMetadataType.INDEX.name, connectionName, followerIndex)
        return replicaionMetadataStore.getMetadata(getReq, timeout).replicationMetadata
    }

    suspend fun getAutofollowMetadata(patternName: String,
                                      connectionName: String): ReplicationMetadata {
        val getReq = GetReplicationMetadataRequest(ReplicationStoreMetadataType.AUTO_FOLLOW.name, connectionName, patternName)
        return replicaionMetadataStore.getMetadata(getReq).replicationMetadata
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
        submitClusterStateUpdateTask(updateReplicationStateDetailsRequest, UpdateReplicationStateDetailsTaskExecutor.INSTANCE
                as ClusterStateTaskExecutor<AcknowledgedRequest<UpdateReplicationStateDetailsRequest>>,
                clusterService,
                "update-replication-state-params")
    }
}
