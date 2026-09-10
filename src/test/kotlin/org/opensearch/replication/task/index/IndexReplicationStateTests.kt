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

package org.opensearch.replication.task.index

import org.assertj.core.api.Assertions.assertThat
import org.opensearch.Version
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.test.OpenSearchTestCase

class IndexReplicationStateTests : OpenSearchTestCase() {

    private val errorMessage = "index [follower-index] replication failed: leader shard unavailable"

    /**
     * Tests same-version serialization: errorMsg must survive a 3.7 -> 3.7 round-trip unchanged.
     */
    fun testFailedStateErrorMsgSerialized() {
        val original = FailedState(emptyMap(), errorMessage)
        BytesStreamOutput().use { out ->
            out.version = Version.CURRENT
            original.writeTo(out)
            val input = out.bytes().streamInput()
            input.version = Version.CURRENT
            val roundtripped = IndexReplicationState.reader(input) as FailedState
            assertThat(roundtripped.errorMsg).isEqualTo(errorMessage)
        }
    }

    /**
     * Tests backward compatibility: the errorMsg field is version-gated on 3.7,
     * so a 3.7 node must neither write nor read it when the peer is older. The field defaults to
     * "" on the receiving side and the stream must round-trip without corruption.
     */
    fun testFailedStateErrorMsgNotSerialized() {
        val original = FailedState(emptyMap(), errorMessage)
        BytesStreamOutput().use { out ->
            out.version = Version.V_2_19_0
            original.writeTo(out)
            val input = out.bytes().streamInput()
            input.version = Version.V_2_19_0
            val roundtripped = IndexReplicationState.reader(input) as FailedState
            assertThat(roundtripped.errorMsg).isEqualTo("")
        }
    }
}
