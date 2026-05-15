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

/**
 * Captures the result of a force resume operation for logging and diagnostics.
 *
 * @param successful Whether the force resume coordinator completed successfully
 * @param followerIndex The name of the follower index being force-resumed
 * @param leaseAcquiredAtSeqNo Map of shardId to the retaining sequence number where the lease was acquired
 * @param durationMillis How long the force resume coordinator took
 * @param failureReason Description of the failure if unsuccessful
 */
data class ForceResumeResult(
    val successful: Boolean,
    val followerIndex: String,
    val leaseAcquiredAtSeqNo: Map<Int, Long>,
    val durationMillis: Long,
    val failureReason: String? = null
)
