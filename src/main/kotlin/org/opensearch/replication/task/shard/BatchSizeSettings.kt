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

package org.opensearch.replication.task.shard

import org.opensearch.index.IndexSettings
import org.opensearch.replication.ReplicationPlugin
import org.opensearch.replication.ReplicationPlugin.Companion.MIN_OPS_BATCH_SIZE
import org.opensearch.replication.ReplicationSettings

/**
 * Helper class to manage batch size settings with fallback from index-level to cluster-level
 */
class BatchSizeSettings(
    private val indexSettings: IndexSettings,
    private val replicationSettings: ReplicationSettings
) {

    /**
     * Get the effective batch size - index-level if set, otherwise cluster-level
     */
    fun getBatchSize(): Int {
        return if (hasIndexLevelSetting()) {
            ReplicationPlugin.REPLICATION_FOLLOWER_OPS_BATCH_SIZE_INDEX.get(indexSettings.settings)
        } else {
            replicationSettings.batchSize
        }
    }

    /**
     * Check if index-level setting is configured
     */
    fun hasIndexLevelSetting(): Boolean {
        return indexSettings.settings.hasValue(ReplicationPlugin.REPLICATION_FOLLOWER_OPS_BATCH_SIZE_INDEX.key)
    }

    /**
     * Get the source of the current batch size setting
     */
    fun getBatchSizeSource(): String {
        return if (hasIndexLevelSetting()) "index-level" else "cluster-level"
    }

    // For dynamic batch size adjustment (2GB fix)
    @Volatile
    private var dynamicBatchSize: Int? = null

    /**
     * Get effective batch size considering dynamic adjustments
     */
    fun getEffectiveBatchSize(): Int {
        return dynamicBatchSize ?: getBatchSize()
    }

    /**
     * Reduce batch size for 2GB limit handling
     */
    fun reduceBatchSize() {
        val currentSize = getEffectiveBatchSize()
        dynamicBatchSize = maxOf(currentSize / 2, MIN_OPS_BATCH_SIZE)
    }

    /**
     * Reset to original batch size after successful operations
     */
    fun resetBatchSize() {
        dynamicBatchSize = null
    }

    /**
     * Check if batch size has been dynamically reduced
     */
    fun isDynamicallyReduced(): Boolean {
        return dynamicBatchSize != null
    }
}