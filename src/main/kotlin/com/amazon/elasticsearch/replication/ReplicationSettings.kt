package com.amazon.elasticsearch.replication

import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.ClusterSettings
import org.elasticsearch.common.unit.ByteSizeValue

class ReplicationSettings(clusterService: ClusterService) {

    @Volatile
    var chunkSize = ReplicationPlugin.REPLICATION_FOLLOWER_RECOVERY_CHUNK_SIZE.get(clusterService.settings)

    @Volatile
    var concurrentFileChunks = ReplicationPlugin.REPLICATION_FOLLOWER_RECOVERY_PARALLEL_CHUNKS.get(clusterService.settings)

    init {
        listenForUpdates(clusterService.clusterSettings)
    }

    private fun listenForUpdates(clusterSettings: ClusterSettings) {
        clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_FOLLOWER_RECOVERY_CHUNK_SIZE) { value: ByteSizeValue -> this.chunkSize = value}
        clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_FOLLOWER_RECOVERY_PARALLEL_CHUNKS) { value: Int -> this.concurrentFileChunks = value}
    }
}
