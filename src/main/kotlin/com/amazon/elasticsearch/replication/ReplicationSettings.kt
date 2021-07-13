package com.amazon.elasticsearch.replication

import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.ClusterSettings
import org.elasticsearch.common.unit.ByteSizeValue
import org.elasticsearch.common.unit.TimeValue

class ReplicationSettings(clusterService: ClusterService) {

    @Volatile var chunkSize = ReplicationPlugin.REPLICATION_FOLLOWER_RECOVERY_CHUNK_SIZE.get(clusterService.settings)
    @Volatile var concurrentFileChunks = ReplicationPlugin.REPLICATION_FOLLOWER_RECOVERY_PARALLEL_CHUNKS.get(clusterService.settings)
    @Volatile var readersPerShard = clusterService.clusterSettings.get(ReplicationPlugin.REPLICATION_PARALLEL_READ_PER_SHARD)
    @Volatile var batchSize = clusterService.clusterSettings.get(ReplicationPlugin.REPLICATION_CHANGE_BATCH_SIZE)
    @Volatile var pollDuration: TimeValue = clusterService.clusterSettings.get(ReplicationPlugin.REPLICATION_PARALLEL_READ_POLL_DURATION)

    init {
        listenForUpdates(clusterService.clusterSettings)
    }

    private fun listenForUpdates(clusterSettings: ClusterSettings) {
        clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_FOLLOWER_RECOVERY_CHUNK_SIZE) { value: ByteSizeValue -> this.chunkSize = value}
        clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_FOLLOWER_RECOVERY_PARALLEL_CHUNKS) { value: Int -> this.concurrentFileChunks = value}
        clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_PARALLEL_READ_PER_SHARD) { value: Int -> this.readersPerShard = value}
        clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_CHANGE_BATCH_SIZE) { batchSize = it }
        clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_PARALLEL_READ_POLL_DURATION) { pollDuration = it }

    }
}
