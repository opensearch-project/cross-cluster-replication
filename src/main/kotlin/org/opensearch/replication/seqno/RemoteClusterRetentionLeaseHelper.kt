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
import org.opensearch.client.Client
import org.opensearch.index.seqno.RetentionLeaseActions
import org.opensearch.index.seqno.RetentionLeaseAlreadyExistsException
import org.opensearch.index.seqno.RetentionLeaseInvalidRetainingSeqNoException
import org.opensearch.index.seqno.RetentionLeaseNotFoundException
import org.opensearch.index.shard.ShardId

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

    public suspend fun addRetentionLease(leaderShardId: ShardId, seqNo: Long, followerShardId: ShardId) {
        val retentionLeaseId = retentionLeaseIdForShard(followerClusterName, followerShardId)
        val request = RetentionLeaseActions.AddRequest(leaderShardId, retentionLeaseId, seqNo, retentionLeaseSource)
        try {
            client.suspendExecute(RetentionLeaseActions.Add.INSTANCE, request)
        } catch (e: RetentionLeaseAlreadyExistsException) {
            log.error("${e.message}")
            log.info("Renew retention lease as it already exists $retentionLeaseId with $seqNo")
            // Only one retention lease should exists for the follower shard
            // Ideally, this should have got cleaned-up
            renewRetentionLease(leaderShardId, seqNo, followerShardId)
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

    public suspend fun attemptRetentionLeaseRemoval(leaderShardId: ShardId, followerShardId: ShardId) {
        val retentionLeaseId = retentionLeaseIdForShard(followerClusterName, followerShardId)
        val request = RetentionLeaseActions.RemoveRequest(leaderShardId, retentionLeaseId)
        try {
            client.suspendExecute(RetentionLeaseActions.Remove.INSTANCE, request)
            log.info("Removed retention lease with id - $retentionLeaseId")
        } catch(e: RetentionLeaseNotFoundException) {
            // log error and bail
            log.error("${e.message}")
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
            log.error("${e.message}")
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
