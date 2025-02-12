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

package org.opensearch.replication.seqno

import org.opensearch.replication.util.suspendExecute
import org.apache.logging.log4j.LogManager
import org.opensearch.action.support.IndicesOptions
import org.opensearch.transport.client.Client
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.seqno.RetentionLeaseActions
import org.opensearch.index.seqno.RetentionLeaseAlreadyExistsException
import org.opensearch.index.seqno.RetentionLeaseInvalidRetainingSeqNoException
import org.opensearch.index.seqno.RetentionLeaseNotFoundException
import org.opensearch.index.shard.IndexShard
import org.opensearch.core.index.shard.ShardId
import org.opensearch.replication.metadata.store.ReplicationMetadata
import org.opensearch.replication.repository.RemoteClusterRepository
import org.opensearch.replication.task.index.IndexReplicationParams
import org.opensearch.replication.util.stackTraceToString
import org.opensearch.replication.util.suspending

class RemoteClusterRetentionLeaseHelper constructor(var followerClusterNameWithUUID: String, val client: Client) {

    private val retentionLeaseSource = retentionLeaseSource(followerClusterNameWithUUID)
    private var followerClusterUUID : String = ""
    private var followerClusterName : String = ""

    constructor(followerClusterName: String, followerClusterUUID: String, client: Client) :this(followerClusterName, client){
        this.followerClusterUUID = followerClusterUUID
        this.followerClusterName = followerClusterName
        this.followerClusterNameWithUUID = getFollowerClusterNameWithUUID(followerClusterName, followerClusterUUID)
    }

    companion object {
        private val log = LogManager.getLogger(RemoteClusterRetentionLeaseHelper::class.java)
        const val RETENTION_LEASE_PREFIX = "replication:"
        fun retentionLeaseSource(followerClusterName: String): String
        = "${RETENTION_LEASE_PREFIX}${followerClusterName}"

        fun retentionLeaseIdForShard(followerClusterName: String, followerShardId: ShardId): String {
            val retentionLeaseSource = retentionLeaseSource(followerClusterName)
            return "$retentionLeaseSource:${followerShardId}"
        }

        fun getFollowerClusterNameWithUUID(followerClusterName: String, followerClusterUUID: String): String{
            return "$followerClusterName:$followerClusterUUID"
        }
    }

    public suspend fun verifyRetentionLeaseExist(leaderShardId: ShardId, followerShardId: ShardId): Boolean  {
        val retentionLeaseId = retentionLeaseIdForShard(followerClusterNameWithUUID, followerShardId)
        // Currently there is no API to describe/list the retention leases .
        // So we are verifying the existence of lease by trying to renew a lease by same name .
        // If retention lease doesn't exist, this will throw an RetentionLeaseNotFoundException exception
        // If it does it will try to RENEW that one with -1 seqno , which should  either
        // throw RetentionLeaseInvalidRetainingSeqNoException if a retention lease exists with higher seq no.
        // which will exist in all probability
        // Or if a retention lease already exists with -1 seqno, it will renew that .
        val request = RetentionLeaseActions.RenewRequest(leaderShardId, retentionLeaseId, RetentionLeaseActions.RETAIN_ALL, retentionLeaseSource)
        try {
            client.suspendExecute(RetentionLeaseActions.Renew.INSTANCE, request)
        } catch (e : RetentionLeaseInvalidRetainingSeqNoException) {
            return true
        }
        catch (e: RetentionLeaseNotFoundException) {
            return addNewRetentionLeaseIfOldExists(leaderShardId, followerShardId, RetentionLeaseActions.RETAIN_ALL)
        }catch (e : Exception) {
            return false
        }
        return true
    }

    private suspend fun addNewRetentionLeaseIfOldExists(leaderShardId: ShardId, followerShardId: ShardId, seqNo: Long): Boolean {
        //Check for old retention lease id
        val oldRetentionLeaseId = retentionLeaseIdForShard(followerClusterName, followerShardId)
        val requestForOldId = RetentionLeaseActions.RenewRequest(leaderShardId, oldRetentionLeaseId, RetentionLeaseActions.RETAIN_ALL, retentionLeaseSource)
        try {
            client.suspendExecute(RetentionLeaseActions.Renew.INSTANCE, requestForOldId)
        } catch (ex: RetentionLeaseInvalidRetainingSeqNoException) {
            //old retention lease id present, will add new retention lease
            log.info("Old retention lease Id ${oldRetentionLeaseId} present with invalid seq number, adding new retention lease with ID:" +
                    "${retentionLeaseIdForShard(followerClusterNameWithUUID, followerShardId)} ")
            return addNewRetentionLease(leaderShardId, seqNo, followerShardId, RemoteClusterRepository.REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC )
        }catch (ex: Exception){
            log.info("Encountered Exception while checking for old retention lease: ${ex.stackTraceToString()}")
            return false
        }
        log.info("Old retention lease Id ${oldRetentionLeaseId}, adding new retention lease with ID:" +
                "${retentionLeaseIdForShard(followerClusterNameWithUUID, followerShardId)} ")
        return  addNewRetentionLease(leaderShardId,seqNo, followerShardId, RemoteClusterRepository.REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC )
    }


    private suspend fun addNewRetentionLease(leaderShardId: ShardId, seqNo: Long, followerShardId: ShardId, timeout: Long): Boolean {
        val retentionLeaseId = retentionLeaseIdForShard(followerClusterNameWithUUID, followerShardId)
        val request = RetentionLeaseActions.AddRequest(leaderShardId, retentionLeaseId, seqNo, retentionLeaseSource)
        try {
            client.suspendExecute(RetentionLeaseActions.Add.INSTANCE, request)
            return true
        } catch (e: Exception) {
            log.info("Exception while adding new retention lease with i: $retentionLeaseId")
            return false
        }
    }

    public suspend fun renewRetentionLease(leaderShardId: ShardId, seqNo: Long, followerShardId: ShardId) {
        val retentionLeaseId = retentionLeaseIdForShard(followerClusterNameWithUUID, followerShardId)
        val request = RetentionLeaseActions.RenewRequest(leaderShardId, retentionLeaseId, seqNo, retentionLeaseSource)
        try {
            client.suspendExecute(RetentionLeaseActions.Renew.INSTANCE, request)
        }catch (e: RetentionLeaseNotFoundException){
            //New retention lease not found, checking presense of old retention lease
            log.info("Retention lease with ID: ${retentionLeaseId} not found," +
                    " checking for old retention lease with ID: ${retentionLeaseIdForShard(followerClusterName, followerShardId)}")
            if(!addNewRetentionLeaseIfOldExists(leaderShardId, followerShardId, seqNo)){
                log.info("Both new $retentionLeaseId and old ${retentionLeaseIdForShard(followerClusterNameWithUUID, followerShardId)} retention lease not found.")
                throw e
            }
        }
    }

    public suspend fun attemptRemoveRetentionLease(clusterService: ClusterService, replMetadata: ReplicationMetadata,
                                                   followerIndexName: String) {
        try {
            val remoteMetadata = getLeaderIndexMetadata(replMetadata.connectionName, replMetadata.leaderContext.resource)
            val params = IndexReplicationParams(replMetadata.connectionName, remoteMetadata.index, followerIndexName)
            val remoteClient = client.getRemoteClusterClient(params.leaderAlias)
            val shards = clusterService.state().routingTable.indicesRouting().get(params.followerIndexName)?.shards()
            val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper( clusterService.clusterName.value(), followerClusterUUID, remoteClient)
            shards?.forEach {
                val followerShardId = it.value.shardId
                log.debug("Removing lease for $followerShardId.id ")
                retentionLeaseHelper.attemptRetentionLeaseRemoval(ShardId(params.leaderIndex, followerShardId.id), followerShardId)
            }
        } catch (e: Exception) {
            log.error("Exception while trying to remove Retention Lease ", e )
        }
    }
    private suspend fun getLeaderIndexMetadata(leaderAlias: String, leaderIndex: String): IndexMetadata {
        val leaderClusterClient = client.getRemoteClusterClient(leaderAlias)
        val clusterStateRequest = leaderClusterClient.admin().cluster().prepareState()
            .clear()
            .setIndices(leaderIndex)
            .setMetadata(true)
            .setIndicesOptions(IndicesOptions.strictSingleIndexNoExpandForbidClosed())
            .request()
        val leaderState = leaderClusterClient.suspending(leaderClusterClient.admin().cluster()::state)(clusterStateRequest).state
        return leaderState.metadata.index(leaderIndex) ?: throw IndexNotFoundException("${leaderAlias}:${leaderIndex}")
    }


    public suspend fun attemptRetentionLeaseRemoval(leaderShardId: ShardId, followerShardId: ShardId) {
        val retentionLeaseId = retentionLeaseIdForShard(followerClusterNameWithUUID, followerShardId)
        val request = RetentionLeaseActions.RemoveRequest(leaderShardId, retentionLeaseId)
        try {
            client.suspendExecute(RetentionLeaseActions.Remove.INSTANCE, request)
            log.info("Removed retention lease with id - $retentionLeaseId")
        } catch(e: RetentionLeaseNotFoundException) {
            // log error and bail
            log.error(e.stackTraceToString())
        } catch (e: Exception) {
            // We are not bubbling up the exception as the stop action/ task cleanup should succeed
            // even if we fail to remove the retention lease from leader cluster
            log.error("Exception in removing retention lease", e)
        }
    }

    public fun attemptRetentionLeaseRemoval(leaderShardId: ShardId, followerShardId: ShardId, timeout: Long) {
        val retentionLeaseId = retentionLeaseIdForShard(followerClusterNameWithUUID, followerShardId)
        val request = RetentionLeaseActions.RemoveRequest(leaderShardId, retentionLeaseId)
        try {
            client.execute(RetentionLeaseActions.Remove.INSTANCE, request).actionGet(timeout)
            log.info("Removed retention lease with id - $retentionLeaseId")
        } catch(e: RetentionLeaseNotFoundException) {
            // log error and bail
            log.error(e.stackTraceToString())
        } catch (e: Exception) {
            // We are not bubbling up the exception as the stop action/ task cleanup should succeed
            // even if we fail to remove the retention lease from leader cluster
            log.error("Exception in removing retention lease", e)
        }
    }


    /**
     * Remove these once the callers are moved to above APIs
     */
    public fun addRetentionLease(leaderShardId: ShardId, seqNo: Long,
                                         followerShardId: ShardId, timeout: Long) {
        val retentionLeaseId = retentionLeaseIdForShard(followerClusterNameWithUUID, followerShardId)
        val request = RetentionLeaseActions.AddRequest(leaderShardId, retentionLeaseId, seqNo, retentionLeaseSource)
        var canRetry = true
        while (true) {
            try {
                log.info("Adding retention lease $retentionLeaseId")
                client.execute(RetentionLeaseActions.Add.INSTANCE, request).actionGet(timeout)
                break
            } catch (e: RetentionLeaseAlreadyExistsException) {
                log.info("Found a stale retention lease $retentionLeaseId on leader.")
                if (canRetry) {
                    canRetry = false
                    attemptRetentionLeaseRemoval(leaderShardId, followerShardId, timeout)
                    log.info("Cleared stale retention lease $retentionLeaseId on leader. Retrying...")
                } else {
                    log.error(e.stackTraceToString())
                    throw e
                }
            }
        }
    }

    public fun renewRetentionLease(leaderShardId: ShardId, seqNo: Long,
                                   followerShardId: ShardId, timeout: Long) {
        val retentionLeaseId = retentionLeaseIdForShard(followerClusterNameWithUUID, followerShardId)
        val request = RetentionLeaseActions.RenewRequest(leaderShardId, retentionLeaseId, seqNo, retentionLeaseSource)
        client.execute(RetentionLeaseActions.Renew.INSTANCE, request).actionGet(timeout)
    }
}
