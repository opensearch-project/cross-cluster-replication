package com.amazon.elasticsearch.replication.metadata

import com.amazon.elasticsearch.replication.ReplicationException
import com.amazon.elasticsearch.replication.action.replicationstatedetails.UpdateReplicationStateAction
import com.amazon.elasticsearch.replication.action.replicationstatedetails.UpdateReplicationStateDetailsRequest
import com.amazon.elasticsearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import com.amazon.elasticsearch.replication.metadata.store.*
import com.amazon.elasticsearch.replication.repository.RemoteClusterRepository
import com.amazon.elasticsearch.replication.util.overrideFgacRole
import com.amazon.elasticsearch.replication.util.suspendExecute
import com.amazon.opendistroforelasticsearch.commons.authuser.User
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.ResourceNotFoundException
import org.elasticsearch.action.DocWriteResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Singleton
import org.elasticsearch.common.settings.Settings

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
                throw ReplicationException("Error updating replication metadata")
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
        } catch (e: ElasticsearchException) {
            log.error("Encountered exception - ", e)
            throw e
        } catch (e: Exception) {
            log.error("Encountered exception - ", e)
            throw ReplicationException(errorMessage)
        }
    }
}
