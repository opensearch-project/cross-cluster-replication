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

package org.opensearch.replication.metadata.store

import org.opensearch.common.unit.TimeValue

/**
 * Centralizes the data-retention rules for STOPPED replication metadata documents that preserve a
 * checkpoint (leaderSequenceNumber/followerAppliedSequence) for role-transition ("warm-attach") resume.
 *
 * Without an enforced retention policy, replication topology and checkpoint sequence numbers preserved
 * on stop (see ReplicationMetadataManager#deleteIndexReplicationMetadata) would remain readable in the
 * `.replication-metadata-store` system index indefinitely. This object provides:
 *  - a single, testable definition of "expired" (isExpired), used both on lazy read
 *    (ReplicationMetadataManager#getIndexReplicationMetadataEnforcingRetention) and by the periodic
 *    background sweep (ReplicationMetadataStore#sweepExpiredCheckpoints), and
 *  - the sweep scheduling interval, kept alongside the expiry rule so the two stay consistent.
 */
object CheckpointRetentionPolicy {

    /**
     * How often ReplicationMetadataStore's background sweep runs to purge expired STOPPED checkpoints.
     * Deliberately coarse-grained relative to the default checkpointRetentionPeriod (24h) — this is a
     * backstop, not the primary enforcement path (which happens lazily on read).
     */
    val SWEEP_INTERVAL: TimeValue = TimeValue.timeValueHours(1)

    /**
     * Returns true if a STOPPED replication metadata document carrying the given
     * [checkpointStoppedAtMillis] timestamp and [checkpointRetentionPeriod] configuration has outlived
     * its retention window as of [nowMillis], and should therefore be purged rather than reused for a
     * role-transition resume or retained further.
     *
     * @param checkpointStoppedAtMillis epoch millis the document was transitioned to STOPPED, or a
     *        non-positive sentinel (e.g. ReplicationMetadata.UNASSIGNED_SEQ_NO) if not applicable.
     * @param checkpointRetentionPeriod a TimeValue-parseable duration string (e.g. "24h").
     */
    fun isExpired(
        checkpointStoppedAtMillis: Long,
        checkpointRetentionPeriod: String,
        nowMillis: Long = System.currentTimeMillis()
    ): Boolean {
        if (checkpointStoppedAtMillis <= 0L) return false
        return try {
            val retentionMillis = TimeValue.parseTimeValue(
                checkpointRetentionPeriod, "checkpoint_retention_period").millis
            nowMillis - checkpointStoppedAtMillis > retentionMillis
        } catch (e: Exception) {
            // Unparseable retention period — fail safe by treating as expired rather than retaining
            // indefinitely.
            true
        }
    }
}

