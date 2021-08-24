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

package org.opensearch.replication

import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.unit.ByteSizeValue
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.utils.OpenForTesting

//ToDo : Make OpenForTesting work
@OpenForTesting
open class ReplicationSettings(clusterService: ClusterService) {

    @Volatile var chunkSize = ReplicationPlugin.REPLICATION_FOLLOWER_RECOVERY_CHUNK_SIZE.get(clusterService.settings)
    @Volatile var concurrentFileChunks = ReplicationPlugin.REPLICATION_FOLLOWER_RECOVERY_PARALLEL_CHUNKS.get(clusterService.settings)
    @Volatile var readersPerShard = clusterService.clusterSettings.get(ReplicationPlugin.REPLICATION_FOLLOWER_CONCURRENT_READERS_PER_SHARD)
    @Volatile var batchSize = clusterService.clusterSettings.get(ReplicationPlugin.REPLICATION_FOLLOWER_OPS_BATCH_SIZE)
    @Volatile var pollDuration: TimeValue = clusterService.clusterSettings.get(ReplicationPlugin.REPLICATION_PARALLEL_READ_POLL_INTERVAL)
    @Volatile var autofollowFetchPollDuration = clusterService.clusterSettings.get(ReplicationPlugin.REPLICATION_AUTOFOLLOW_REMOTE_INDICES_POLL_INTERVAL)
    @Volatile var autofollowRetryPollDuration = clusterService.clusterSettings.get(ReplicationPlugin.REPLICATION_AUTOFOLLOW_REMOTE_INDICES_RETRY_POLL_INTERVAL)
    @Volatile var metadataSyncInterval = clusterService.clusterSettings.get(ReplicationPlugin.REPLICATION_METADATA_SYNC_INTERVAL)
    init {
        listenForUpdates(clusterService.clusterSettings)
    }

    private fun listenForUpdates(clusterSettings: ClusterSettings) {
        clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_FOLLOWER_RECOVERY_CHUNK_SIZE) { value: ByteSizeValue -> this.chunkSize = value}
        clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_FOLLOWER_RECOVERY_PARALLEL_CHUNKS) { value: Int -> this.concurrentFileChunks = value}
        clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_FOLLOWER_CONCURRENT_READERS_PER_SHARD) { value: Int -> this.readersPerShard = value}
        clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_FOLLOWER_OPS_BATCH_SIZE) { batchSize = it }
        clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_PARALLEL_READ_POLL_INTERVAL) { pollDuration = it }
        clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_AUTOFOLLOW_REMOTE_INDICES_POLL_INTERVAL) { autofollowFetchPollDuration = it }
        clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_AUTOFOLLOW_REMOTE_INDICES_RETRY_POLL_INTERVAL) { autofollowRetryPollDuration = it }
        clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_METADATA_SYNC_INTERVAL) { metadataSyncInterval = it }
    }
}
