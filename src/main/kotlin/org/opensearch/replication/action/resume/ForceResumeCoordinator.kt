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
import org.opensearch.action.admin.indices.close.CloseIndexRequest
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsAction
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.cluster.service.ClusterService
import org.opensearch.core.index.shard.ShardId
import org.opensearch.index.seqno.RetentionLeaseAlreadyExistsException
import org.opensearch.replication.action.index.block.IndexBlockUpdateType
import org.opensearch.replication.action.index.block.UpdateIndexBlockAction
import org.opensearch.replication.action.index.block.UpdateIndexBlockRequest
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.repository.RemoteClusterRepository
import org.opensearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import org.opensearch.replication.task.index.IndexReplicationParams
import org.opensearch.replication.util.suspendExecute
import org.opensearch.replication.util.suspending
import org.opensearch.transport.client.Client

/**
 * Coordinates the force resume operation when retention leases have expired.
 *
 * The flow is:
 * 1. Validate replication is PAUSED
 * 2. Remove the index block on the follower so we can operate on it
 * 3. Acquire retention leases on the leader BEFORE deleting the follower (prevents race condition)
 * 4. Close and delete the follower index
 * 5. On any failure after block removal: clean up leases and re-add the block
 *
 * After this coordinator completes, the caller starts an IndexReplicationTask.
 * Since the follower index no longer exists, isResumed() returns false,
 * which triggers setupAndStartRestore() -> snapshot bootstrap from leader.
 */
class ForceResumeCoordinator(
    private val client: Client,
    private val clusterService: ClusterService,
    private val replicationMetadataManager: ReplicationMetadataManager
) {
    companion object {
        private val log = LogManager.getLogger(ForceResumeCoordinator::class.java)
    }

    /**
     * Executes the force resume workflow. Returns a [ForceResumeResult] with details.
     * Throws on unrecoverable failure (after cleaning up).
     */
    suspend fun executeForceResume(params: IndexReplicationParams): ForceResumeResult {
        val followerIndex = params.followerIndexName
        val startTime = System.currentTimeMillis()
        val acquiredLeases = mutableMapOf<Int, Long>()

        log.info("Starting force resume for index $followerIndex")

        // Step 1: Remove the replication block so we can operate on the follower index.
        // If this fails, nothing has been modified — safe to propagate directly.
        removeIndexBlock(followerIndex)

        // From this point, any failure must clean up acquired leases and re-add the block.
        try {
            // Step 2: Acquire retention leases on the leader BEFORE restore.
            // This prevents the race condition where the leader's global checkpoint
            // advances past the snapshot point before shard tasks can establish leases.
            acquirePreRestoreRetentionLeases(params, acquiredLeases)

            // Step 3: Close and delete the follower index.
            // This makes isResumed() return false so IndexReplicationTask
            // goes through INIT -> setupAndStartRestore() -> RESTORING -> ...
            prepareSnapshotBootstrap(followerIndex)
        } catch (e: Exception) {
            log.error("Force resume failed for $followerIndex, cleaning up", e)
            cleanupOnFailure(params, acquiredLeases, followerIndex)
            throw e
        }

        val duration = System.currentTimeMillis() - startTime
        log.info("Force resume coordinator completed for $followerIndex in ${duration}ms. " +
                "Leases acquired for shards: ${acquiredLeases.keys}")

        return ForceResumeResult(
            successful = true,
            followerIndex = followerIndex,
            leaseAcquiredAtSeqNo = acquiredLeases.toMap(),
            durationMillis = duration
        )
    }

    // Removes the replication index block so we can close/delete the follower.
    private suspend fun removeIndexBlock(followerIndex: String) {
        log.info("Removing index block for force resume on $followerIndex")
        try {
            val request = UpdateIndexBlockRequest(followerIndex, IndexBlockUpdateType.REMOVE_BLOCK)
            client.suspendExecute(UpdateIndexBlockAction.INSTANCE, request, defaultContext = true)
            log.info("Removed index block for $followerIndex")
        } catch (e: Exception) {
            log.error("Failed to remove index block for $followerIndex during force resume", e)
            throw IllegalStateException(
                "Force resume failed: unable to remove index block for $followerIndex. " +
                        "Index remains in PAUSED state. Error: ${e.message}", e
            )
        }
    }

    /**
     * Acquires retention leases on the leader for each shard BEFORE deleting the follower.
     * Tracks which shards got leases in [acquiredLeases] for cleanup on partial failure.
     */
    private suspend fun acquirePreRestoreRetentionLeases(
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

        if (shards == null || shards.isEmpty()) {
            log.warn("No shards found for follower index ${params.followerIndexName}")
            return
        }

        shards.forEach { entry ->
            val followerShardId = entry.value.shardId
            val leaderShardId = ShardId(params.leaderIndex, followerShardId.id)

            // Get the leader's current global checkpoint for this shard
            val leaderCheckpoint = getLeaderGlobalCheckpoint(
                remoteClient, params.leaderIndex.name, leaderShardId.id
            )

            // Acquire lease at leader's current checkpoint + 1
            // This ensures all operations from this point forward are retained
            val retainingSeqNo = leaderCheckpoint + 1

            try {
                retentionLeaseHelper.addRetentionLease(
                    leaderShardId, retainingSeqNo, followerShardId,
                    RemoteClusterRepository.REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC
                )
                acquiredLeases[followerShardId.id] = retainingSeqNo
                log.info("Acquired pre-restore retention lease for shard ${followerShardId.id} " +
                        "at seqNo $retainingSeqNo")
            } catch (e: RetentionLeaseAlreadyExistsException) {
                // Lease already exists (maybe from a previous attempt), renew it
                retentionLeaseHelper.renewRetentionLease(
                    leaderShardId, retainingSeqNo, followerShardId,
                    RemoteClusterRepository.REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC
                )
                acquiredLeases[followerShardId.id] = retainingSeqNo
                log.info("Renewed pre-restore retention lease for shard ${followerShardId.id} " +
                        "at seqNo $retainingSeqNo")
            }
        }

        log.info("Acquired pre-restore retention leases for all ${acquiredLeases.size} shards " +
                "of ${params.followerIndexName}")
    }

    // Retrieves the leader shard's global checkpoint via IndicesStats.
    private suspend fun getLeaderGlobalCheckpoint(
        remoteClient: Client,
        leaderIndexName: String,
        shardId: Int
    ): Long {
        val statsRequest = IndicesStatsRequest().all().indices(leaderIndexName)
        val statsResponse = remoteClient.suspendExecute(
            IndicesStatsAction.INSTANCE, statsRequest, injectSecurityContext = true
        )

        for (shardStats in statsResponse.shards) {
            if (shardStats.shardRouting.shardId().id == shardId && shardStats.shardRouting.primary()) {
                val globalCheckpoint = shardStats.seqNoStats?.globalCheckpoint
                    ?: throw IllegalStateException(
                        "Unable to get global checkpoint for leader shard $leaderIndexName[$shardId]"
                    )
                log.debug("Leader shard $leaderIndexName[$shardId] global checkpoint: $globalCheckpoint")
                return globalCheckpoint
            }
        }

        throw IllegalStateException(
            "Primary shard $shardId not found for leader index $leaderIndexName"
        )
    }

    // Closes and deletes the follower index to trigger snapshot bootstrap.
    private suspend fun prepareSnapshotBootstrap(followerIndex: String) {
        log.info("Preparing snapshot bootstrap: closing and deleting follower index $followerIndex")

        // Close the index first (best-effort — it may already be closed)
        try {
            val closeRequest = CloseIndexRequest(followerIndex)
            client.suspending(client.admin().indices()::close, defaultContext = true)(closeRequest)
            log.info("Closed follower index $followerIndex")
        } catch (e: Exception) {
            log.warn("Failed to close follower index $followerIndex " +
                    "(may already be closed): ${e.message}")
        }

        // Delete the follower index — this makes isResumed() return false
        val deleteRequest = DeleteIndexRequest(followerIndex)
        client.suspending(
            client.admin().indices()::delete, defaultContext = true
        )(deleteRequest)
        log.info("Deleted follower index $followerIndex — " +
                "IndexReplicationTask will restore from snapshot")
    }

    // Cleans up on failure: removes any partially acquired leases and re-adds the index block.
    private suspend fun cleanupOnFailure(
        params: IndexReplicationParams,
        acquiredLeases: Map<Int, Long>,
        followerIndex: String
    ) {
        // Clean up any partially acquired retention leases
        if (acquiredLeases.isNotEmpty()) {
            cleanupAcquiredLeases(params, acquiredLeases)
        }

        // Re-add the index block to preserve follower state
        restoreBlockOnFailure(followerIndex)
    }

    // Removes retention leases that were acquired during a failed force resume attempt.
    private suspend fun cleanupAcquiredLeases(
        params: IndexReplicationParams,
        acquiredLeases: Map<Int, Long>
    ) {
        log.info("Cleaning up ${acquiredLeases.size} partially acquired retention leases " +
                "for ${params.followerIndexName}")
        try {
            val remoteClient = client.getRemoteClusterClient(params.leaderAlias)
            val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(
                clusterService.clusterName.value(),
                clusterService.state().metadata.clusterUUID(),
                remoteClient
            )

            for ((shardIdInt, _) in acquiredLeases) {
                val followerShardId = ShardId(
                    clusterService.state().metadata.index(params.followerIndexName)?.index
                        ?: continue,
                    shardIdInt
                )
                val leaderShardId = ShardId(params.leaderIndex, shardIdInt)
                try {
                    retentionLeaseHelper.attemptRetentionLeaseRemoval(leaderShardId, followerShardId)
                    log.info("Cleaned up retention lease for shard $shardIdInt")
                } catch (e: Exception) {
                    // Best-effort cleanup — lease will expire naturally in 12h
                    log.warn("Failed to clean up retention lease for shard $shardIdInt: ${e.message}")
                }
            }
        } catch (e: Exception) {
            log.warn("Failed to clean up retention leases for ${params.followerIndexName}: ${e.message}")
        }
    }

    /**
     * Re-adds the replication index block after a failed force resume attempt.
     * This preserves the follower index in a consistent PAUSED state.
     */
    private suspend fun restoreBlockOnFailure(followerIndex: String) {
        try {
            log.info("Re-adding index block for $followerIndex after force resume failure")
            val request = UpdateIndexBlockRequest(followerIndex, IndexBlockUpdateType.ADD_BLOCK)
            client.suspendExecute(UpdateIndexBlockAction.INSTANCE, request, defaultContext = true)
            log.info("Restored index block for $followerIndex")
        } catch (e: Exception) {
            // This is a serious situation — the follower is unblocked but force resume failed.
            // Log at error level so operators can investigate.
            log.error("CRITICAL: Failed to re-add index block for $followerIndex after force resume failure. " +
                    "The index may be in an inconsistent state. Manual intervention may be required.", e)
        }
    }
}
