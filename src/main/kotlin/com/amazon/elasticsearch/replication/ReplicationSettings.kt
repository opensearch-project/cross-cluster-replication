package com.amazon.elasticsearch.replication

import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.ClusterSettings
import org.elasticsearch.common.unit.ByteSizeValue
import org.elasticsearch.common.unit.TimeValue

class ReplicationSettings(clusterService: ClusterService) {

    @Volatile var chunkSize = ReplicationPlugin.REPLICATION_FOLLOWER_RECOVERY_CHUNK_SIZE.get(clusterService.settings)
    @Volatile var concurrentFileChunks = ReplicationPlugin.REPLICATION_FOLLOWER_RECOVERY_PARALLEL_CHUNKS.get(clusterService.settings)
    @Volatile var readersPerShard = clusterService.clusterSettings.get(ReplicationPlugin.REPLICATION_FOLLOWER_CONCURRENT_READERS_PER_SHARD)
    @Volatile var batchSize = clusterService.clusterSettings.get(ReplicationPlugin.REPLICATION_FOLLOWER_OPS_BATCH_SIZE)
    @Volatile var pollDuration: TimeValue = clusterService.clusterSettings.get(ReplicationPlugin.REPLICATION_PARALLEL_READ_POLL_INTERVAL)
    @Volatile var autofollowFetchPollDuration = clusterService.clusterSettings.get(ReplicationPlugin.REPLICATION_AUTOFOLLOW_REMOTE_INDICES_POLL_INTERVAL)
    @Volatile var autofollowRetryPollDuration = clusterService.clusterSettings.get(ReplicationPlugin.REPLICATION_AUTOFOLLOW_REMOTE_INDICES_RETRY_POLL_INTERVAL)
    @Volatile var metadataSyncInterval = clusterService.clusterSettings.get(ReplicationPlugin.REPLICATION_METADATA_SYNC_INTERVAL)
    @Volatile var leaseRenewalMaxFailureDuration: TimeValue = clusterService.clusterSettings.get(ReplicationPlugin.REPLICATION_RETENTION_LEASE_MAX_FAILURE_DURATION)
    @Volatile var followerBlockStart: Boolean = clusterService.clusterSettings.get(ReplicationPlugin.REPLICATION_FOLLOWER_BLOCK_START)

    init {
        listenForUpdates(clusterService.clusterSettings)
    }

    private fun listenForUpdates(clusterSettings: ClusterSettings) {
        clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_FOLLOWER_RECOVERY_CHUNK_SIZE) { value: ByteSizeValue -> this.chunkSize = value}
        clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_FOLLOWER_RECOVERY_PARALLEL_CHUNKS) { value: Int -> this.concurrentFileChunks = value}
        clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_FOLLOWER_CONCURRENT_READERS_PER_SHARD) { value: Int -> this.readersPerShard = value}
        clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_FOLLOWER_OPS_BATCH_SIZE) { batchSize = it }
        clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_PARALLEL_READ_POLL_INTERVAL) { pollDuration = it }
        clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_RETENTION_LEASE_MAX_FAILURE_DURATION) { leaseRenewalMaxFailureDuration = it }
        clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_AUTOFOLLOW_REMOTE_INDICES_POLL_INTERVAL) { autofollowFetchPollDuration = it }
        clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_AUTOFOLLOW_REMOTE_INDICES_RETRY_POLL_INTERVAL) { autofollowRetryPollDuration = it }
        clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_METADATA_SYNC_INTERVAL) { metadataSyncInterval = it }
        clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_FOLLOWER_BLOCK_START) { followerBlockStart = it }
    }
}
