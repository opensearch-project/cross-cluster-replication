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

package org.opensearch.replication.seqno

import org.apache.logging.log4j.LogManager
import org.opensearch.client.Client
import org.opensearch.common.logging.Loggers
import org.opensearch.index.seqno.RetentionLeaseActions
import org.opensearch.index.seqno.RetentionLeaseAlreadyExistsException
import org.opensearch.index.seqno.RetentionLeaseNotFoundException
import org.opensearch.index.shard.ShardId
import org.opensearch.replication.util.suspendExecute

class RemoteClusterRetentionLeaseHelper constructor(val followerClusterName: String, val client: Client) {

    private val retentionLeaseSource = retentionLeaseSource(followerClusterName)

    companion object {
        private val log = LogManager.getLogger(RemoteClusterRetentionLeaseHelper::class.java)
        fun retentionLeaseSource(followerClusterName: String): String = "replication:${followerClusterName}"

        fun retentionLeaseIdForShard(followerClusterName: String, followerShardId: ShardId): String {
            val retentionLeaseSource = retentionLeaseSource(followerClusterName)
            return "$retentionLeaseSource:${followerShardId}"
        }
    }

    public suspend fun addRetentionLease(remoteShardId: ShardId, seqNo: Long, followerShardId: ShardId) {
        val retentionLeaseId = retentionLeaseIdForShard(followerClusterName, followerShardId)
        val request = RetentionLeaseActions.AddRequest(remoteShardId, retentionLeaseId, seqNo, retentionLeaseSource)
        try {
            client.suspendExecute(RetentionLeaseActions.Add.INSTANCE, request)
        } catch (e: RetentionLeaseAlreadyExistsException) {
            log.error("${e.message}")
            log.info("Renew retention lease as it already exists $retentionLeaseId with $seqNo")
            // Only one retention lease should exists for the follower shard
            // Ideally, this should have got cleaned-up
            renewRetentionLease(remoteShardId, seqNo, followerShardId)
        }
    }

    public suspend fun renewRetentionLease(remoteShardId: ShardId, seqNo: Long, followerShardId: ShardId) {
        val retentionLeaseId = retentionLeaseIdForShard(followerClusterName, followerShardId)
        val request = RetentionLeaseActions.RenewRequest(remoteShardId, retentionLeaseId, seqNo, retentionLeaseSource)
        client.suspendExecute(RetentionLeaseActions.Renew.INSTANCE, request)
    }

    public suspend fun removeRetentionLease(remoteShardId: ShardId, followerShardId: ShardId) {
        val retentionLeaseId = retentionLeaseIdForShard(followerClusterName, followerShardId)
        val request = RetentionLeaseActions.RemoveRequest(remoteShardId, retentionLeaseId)
        try {
            client.suspendExecute(RetentionLeaseActions.Remove.INSTANCE, request)
            log.info("Removed retention lease with id - $retentionLeaseId")
        } catch(e: RetentionLeaseNotFoundException) {
            // log error and bail
            log.error("${e.message}")
        }
    }


    /**
     * Remove these once the callers are moved to above APIs
     */
    public fun addRetentionLease(remoteShardId: ShardId, seqNo: Long,
                                 followerShardId: ShardId, timeout: Long) {
        val retentionLeaseId = retentionLeaseIdForShard(followerClusterName, followerShardId)
        val request = RetentionLeaseActions.AddRequest(remoteShardId, retentionLeaseId, seqNo, retentionLeaseSource)
        try {
            client.execute(RetentionLeaseActions.Add.INSTANCE, request).actionGet(timeout)
        } catch (e: RetentionLeaseAlreadyExistsException) {
            log.error("${e.message}")
            log.info("Renew retention lease as it already exists $retentionLeaseId with $seqNo")
            // Only one retention lease should exists for the follower shard
            // Ideally, this should have got cleaned-up
            renewRetentionLease(remoteShardId, seqNo, followerShardId, timeout)
        }
    }

    public fun renewRetentionLease(remoteShardId: ShardId, seqNo: Long,
                                   followerShardId: ShardId, timeout: Long) {
        val retentionLeaseId = retentionLeaseIdForShard(followerClusterName, followerShardId)
        val request = RetentionLeaseActions.RenewRequest(remoteShardId, retentionLeaseId, seqNo, retentionLeaseSource)
        client.execute(RetentionLeaseActions.Renew.INSTANCE, request).actionGet(timeout)
    }
}
