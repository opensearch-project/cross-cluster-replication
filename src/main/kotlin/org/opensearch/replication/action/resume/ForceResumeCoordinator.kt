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

package org.opensearch.replication.action.resume

import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsAction
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.cluster.service.ClusterService
import org.opensearch.core.index.shard.ShardId
import org.opensearch.index.seqno.RetentionLeaseAlreadyExistsException
import org.opensearch.replication.action.index.block.IndexBlockUpdateType
import org.opensearch.replication.action.index.block.UpdateIndexBlockAction
import org.opensearch.replication.action.index.block.UpdateIndexBlockRequest
import org.opensearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import org.opensearch.replication.task.index.IndexReplicationParams
import org.opensearch.replication.util.suspendExecute
import org.opensearch.replication.util.suspending
import org.opensearch.transport.client.Client

/**
 * Coordinates the force resume operation when retention leases have expired.
 * Reuses existing infrastructure:
 * Block removal/addition: same as TransportStopIndexReplicationAction
 * Index deletion: same as IndexReplicationTask.cancelRestore()
 * Lease add/renew/remove: existing RemoteClusterRetentionLeaseHelper methods
 * The only new logic is acquiring retention leases at leaderGlobalCheckpoint+1
 * BEFORE deleting the follower index, which prevents the race condition where
 * the leader's translog is purged during the async snapshot restore.
 */
class ForceResumeCoordinator(
    private val client: Client,
    private val clusterService: ClusterService
) {
    companion object {
        private val log = LogManager.getLogger(ForceResumeCoordinator::class.java)
    }

    /**
     * Executes the force resume workflow:
     * 1. Remove index block (reuses UpdateIndexBlockAction — same as stop action)
     * 2. Acquire retention leases at leaderCheckpoint+1 per shard (NEW — race condition fix)
     * 3. Delete follower index (reuses same pattern as IndexReplicationTask.cancelRestore)
     */
    suspend fun executeForceResume(params: IndexReplicationParams): ForceResumeResult {
        val followerIndex = params.followerIndexName
        val startTime = System.currentTimeMillis()
        val acquiredLeases = mutableMapOf<Int, Long>()

        log.info("Starting force resume for index $followerIndex")

        // Step 1: Remove index block — same pattern as TransportStopIndexReplicationAction#L103
        removeIndexBlock(followerIndex)

        // From this point, any failure must clean up leases and re-add the block.
        try {
            // Step 2: Acquire retention leases on leader BEFORE deleting follower.
            // This is the only genuinely new logic — prevents the race condition.
            acquirePreRestoreLeases(params, acquiredLeases)

            // Step 3: Delete follower index — same pattern as IndexReplicationTask.cancelRestore()
            deleteFollowerIndex(followerIndex)
        } catch (e: Exception) {
            log.error("Force resume failed for $followerIndex, cleaning up", e)
            cleanupOnFailure(params, acquiredLeases, followerIndex)
            throw e
        }

        val duration = System.currentTimeMillis() - startTime
        log.info("Force resume completed for $followerIndex in ${duration}ms")

        return ForceResumeResult(
            successful = true,
            followerIndex = followerIndex,
            leaseAcquiredAtSeqNo = acquiredLeases.toMap(),
            durationMillis = duration
        )
    }

    private suspend fun removeIndexBlock(followerIndex: String) {
        log.info("Removing index block for $followerIndex")
        val request = UpdateIndexBlockRequest(followerIndex, IndexBlockUpdateType.REMOVE_BLOCK)
        client.suspendExecute(UpdateIndexBlockAction.INSTANCE, request, defaultContext = true)
    }

    /**
    Acquires retention leases at leaderGlobalCheckpoint+1 per shard BEFORE
     * deleting the follower. This prevents the race condition where the leader's
     * global checkpoint advances during restore and operations get purged.
     */
    private suspend fun acquirePreRestoreLeases(
        params: IndexReplicationParams,
        acquiredLeases: MutableMap<Int, Long>
    ) {
        val remoteClient = client.getRemoteClusterClient(params.leaderAlias)
        val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(
            clusterService.clusterName.value(),
            clusterService.state().metadata.clusterUUID(),
            remoteClient
        )

        val shards = clusterService.state().routingTable
            .indicesRouting().get(params.followerIndexName)?.shards()
            ?: return

        shards.forEach { entry ->
            val followerShardId = entry.value.shardId
            val leaderShardId = ShardId(params.leaderIndex, followerShardId.id)
            val retainingSeqNo = getLeaderGlobalCheckpoint(remoteClient, params.leaderIndex.name, leaderShardId.id) + 1

            try {
                retentionLeaseHelper.addRetentionLease(
                    leaderShardId, retainingSeqNo, followerShardId
                )
            } catch (e: RetentionLeaseAlreadyExistsException) {
                retentionLeaseHelper.renewRetentionLease(
                    leaderShardId, retainingSeqNo, followerShardId
                )
            }
            acquiredLeases[followerShardId.id] = retainingSeqNo
            log.info("Acquired pre-restore lease for shard ${followerShardId.id} at seqNo $retainingSeqNo")
        }
    }

    // Fetches the leader shard's global checkpoint via IndicesStatsAction.
    private suspend fun getLeaderGlobalCheckpoint(remoteClient: Client, leaderIndexName: String, shardId: Int): Long {
        val statsResponse = remoteClient.suspendExecute(
            IndicesStatsAction.INSTANCE,
            IndicesStatsRequest().all().indices(leaderIndexName),
            injectSecurityContext = true
        )
        return statsResponse.shards
            .firstOrNull { it.shardRouting.shardId().id == shardId && it.shardRouting.primary() }
            ?.seqNoStats?.globalCheckpoint
            ?: throw IllegalStateException("Primary shard $shardId not found for leader index $leaderIndexName")
    }

    /**
     * Same pattern as IndexReplicationTask.cancelRestore() — deletes the follower index.
     * This makes isResumed() return false, triggering setupAndStartRestore() in the state machine.
     */
    private suspend fun deleteFollowerIndex(followerIndex: String) {
        log.info("Deleting follower index $followerIndex for snapshot bootstrap")
        client.suspending(client.admin().indices()::delete, defaultContext = true)(DeleteIndexRequest(followerIndex))
    }

    /**
     * Cleanup on failure: remove partially acquired leases + re-add index block.
     * Uses existing RemoteClusterRetentionLeaseHelper.attemptRetentionLeaseRemoval()
     * and UpdateIndexBlockAction with ADD_BLOCK.
     */
    private suspend fun cleanupOnFailure(
        params: IndexReplicationParams,
        acquiredLeases: Map<Int, Long>,
        followerIndex: String
    ) {
        // Clean up partially acquired leases — uses existing attemptRetentionLeaseRemoval()
        if (acquiredLeases.isNotEmpty()) {
            try {
                val remoteClient = client.getRemoteClusterClient(params.leaderAlias)
                val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(
                    clusterService.clusterName.value(),
                    clusterService.state().metadata.clusterUUID(),
                    remoteClient
                )
                for (shardIdInt in acquiredLeases.keys) {
                    val followerShardId = ShardId(
                        clusterService.state().metadata.index(params.followerIndexName)?.index ?: continue,
                        shardIdInt
                    )
                    retentionLeaseHelper.attemptRetentionLeaseRemoval(ShardId(params.leaderIndex, shardIdInt), followerShardId)
                }
            } catch (e: Exception) {
                log.warn("Best-effort lease cleanup failed for ${params.followerIndexName}: ${e.message}")
            }
        }

        // Re-add index block — same pattern as IndexReplicationTask.addIndexBlockForReplication()
        try {
            val request = UpdateIndexBlockRequest(followerIndex, IndexBlockUpdateType.ADD_BLOCK)
            client.suspendExecute(UpdateIndexBlockAction.INSTANCE, request, defaultContext = true)
            log.info("Restored index block for $followerIndex")
        } catch (e: Exception) {
            log.error("CRITICAL: Failed to re-add index block for $followerIndex. Manual intervention may be required.", e)
        }
    }
}
