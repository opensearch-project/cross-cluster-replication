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

package com.amazon.elasticsearch.replication.seqno

import com.amazon.elasticsearch.replication.metadata.store.ReplicationMetadata
import com.amazon.elasticsearch.replication.task.index.IndexReplicationParams
import com.amazon.elasticsearch.replication.util.stackTraceToString
import com.amazon.elasticsearch.replication.util.suspending
import com.amazon.elasticsearch.replication.util.suspendExecute
import org.apache.logging.log4j.LogManager

import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.index.seqno.RetentionLeaseActions
import org.elasticsearch.index.seqno.RetentionLeaseAlreadyExistsException
import org.elasticsearch.index.seqno.RetentionLeaseInvalidRetainingSeqNoException
import org.elasticsearch.index.seqno.RetentionLeaseNotFoundException
import org.elasticsearch.index.shard.ShardId

class RemoteClusterRetentionLeaseHelper constructor(val followerClusterName: String, val client: Client) {

    private val retentionLeaseSource = retentionLeaseSource(followerClusterName)

    companion object {
        private val log = LogManager.getLogger(RemoteClusterRetentionLeaseHelper::class.java)
        const val RETENTION_LEASE_PREFIX = "replication:"
        fun retentionLeaseSource(followerClusterName: String): String = "${RETENTION_LEASE_PREFIX}${followerClusterName}"

        fun retentionLeaseIdForShard(followerClusterName: String, followerShardId: ShardId): String {
            val retentionLeaseSource = retentionLeaseSource(followerClusterName)
            return "$retentionLeaseSource:${followerShardId}"
        }
    }

    public suspend fun verifyRetentionLeaseExist(leaderShardId: ShardId, followerShardId: ShardId): Boolean  {
        val retentionLeaseId = retentionLeaseIdForShard(followerClusterName, followerShardId)
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
            return false
        }
        return true
    }

    public suspend fun renewRetentionLease(leaderShardId: ShardId, seqNo: Long, followerShardId: ShardId) {
        val retentionLeaseId = retentionLeaseIdForShard(followerClusterName, followerShardId)
        val request = RetentionLeaseActions.RenewRequest(leaderShardId, retentionLeaseId, seqNo, retentionLeaseSource)
        client.suspendExecute(RetentionLeaseActions.Renew.INSTANCE, request)
    }

    public suspend fun attemptRemoveRetentionLease(clusterService: ClusterService, replMetadata: ReplicationMetadata,
                                                   followerIndexName: String) {
        try {
            val remoteMetadata = getLeaderIndexMetadata(replMetadata.connectionName, replMetadata.leaderContext.resource)
            val params = IndexReplicationParams(replMetadata.connectionName, remoteMetadata.index, followerIndexName)
            val remoteClient = client.getRemoteClusterClient(params.leaderAlias)
            val shards = clusterService.state().routingTable.indicesRouting().get(params.followerIndexName).shards()
            val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(clusterService.clusterName.value(), remoteClient)
            shards.forEach {
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
        val retentionLeaseId = retentionLeaseIdForShard(followerClusterName, followerShardId)
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


    /**
     * Remove these once the callers are moved to above APIs
     */
    public fun addRetentionLease(leaderShardId: ShardId, seqNo: Long,
                                 followerShardId: ShardId, timeout: Long) {
        val retentionLeaseId = retentionLeaseIdForShard(followerClusterName, followerShardId)
        val request = RetentionLeaseActions.AddRequest(leaderShardId, retentionLeaseId, seqNo, retentionLeaseSource)
        try {
            client.execute(RetentionLeaseActions.Add.INSTANCE, request).actionGet(timeout)
        } catch (e: RetentionLeaseAlreadyExistsException) {
            log.error(e.stackTraceToString())
            log.info("Renew retention lease as it already exists $retentionLeaseId with $seqNo")
            // Only one retention lease should exists for the follower shard
            // Ideally, this should have got cleaned-up
            renewRetentionLease(leaderShardId, seqNo, followerShardId, timeout)
        }
    }

    public fun renewRetentionLease(leaderShardId: ShardId, seqNo: Long,
                                   followerShardId: ShardId, timeout: Long) {
        val retentionLeaseId = retentionLeaseIdForShard(followerClusterName, followerShardId)
        val request = RetentionLeaseActions.RenewRequest(leaderShardId, retentionLeaseId, seqNo, retentionLeaseSource)
        client.execute(RetentionLeaseActions.Renew.INSTANCE, request).actionGet(timeout)
    }
}
