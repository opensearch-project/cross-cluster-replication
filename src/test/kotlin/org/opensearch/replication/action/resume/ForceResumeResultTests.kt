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

import org.opensearch.test.OpenSearchTestCase

class ForceResumeResultTests : OpenSearchTestCase() {

    fun `test successful result`() {
        val leases = mapOf(0 to 100L, 1 to 200L)
        val result = ForceResumeResult(
            successful = true,
            followerIndex = "follower-index",
            leaseAcquiredAtSeqNo = leases,
            durationMillis = 500L
        )
        assertTrue(result.successful)
        assertEquals("follower-index", result.followerIndex)
        assertEquals(leases, result.leaseAcquiredAtSeqNo)
        assertEquals(500L, result.durationMillis)
        assertNull(result.failureReason)
    }

    fun `test failed result with reason`() {
        val result = ForceResumeResult(
            successful = false,
            followerIndex = "follower-index",
            leaseAcquiredAtSeqNo = emptyMap(),
            durationMillis = 100L,
            failureReason = "Retention lease acquisition failed"
        )
        assertFalse(result.successful)
        assertEquals("Retention lease acquisition failed", result.failureReason)
        assertTrue(result.leaseAcquiredAtSeqNo.isEmpty())
    }

    fun `test result equality`() {
        val leases = mapOf(0 to 50L)
        val result1 = ForceResumeResult(true, "idx", leases, 10L)
        val result2 = ForceResumeResult(true, "idx", leases, 10L)
        assertEquals(result1, result2)
        assertEquals(result1.hashCode(), result2.hashCode())
    }

    fun `test result inequality on different follower index`() {
        val result1 = ForceResumeResult(true, "idx-a", emptyMap(), 10L)
        val result2 = ForceResumeResult(true, "idx-b", emptyMap(), 10L)
        assertNotEquals(result1, result2)
    }

}
