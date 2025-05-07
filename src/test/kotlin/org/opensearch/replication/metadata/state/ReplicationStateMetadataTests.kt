/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.replication.metadata.state

import org.assertj.core.api.Assertions.assertThat
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.replication.metadata.ReplicationOverallState
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.XContentTestUtils

class ReplicationStateMetadataTests : OpenSearchTestCase() {

    companion object {
        const val INDEX = "index"
    }

    fun `test serialization`() {
        val expected = ReplicationStateMetadata.EMPTY
            .addReplicationStateParams(INDEX, mapOf(Pair(REPLICATION_LAST_KNOWN_OVERALL_STATE, ReplicationOverallState.RUNNING.name)))
        val output = BytesStreamOutput()
        expected.writeTo(output)
        val deserialized = ReplicationStateMetadata(output.bytes().streamInput())
        assertEquals(expected, deserialized)
    }

    fun `test json serialization`() {
        val expected = ReplicationStateMetadata.EMPTY
            .addReplicationStateParams(INDEX, mapOf(Pair(REPLICATION_LAST_KNOWN_OVERALL_STATE, ReplicationOverallState.RUNNING.name)))
        val actual = XContentTestUtils.convertToMap(expected)
        assertThat(actual).containsKey("replication_details")
        assertThat(actual["replication_details"] as Map<String, Map<String, String?>>)
            .containsEntry(INDEX, mapOf(Pair(REPLICATION_LAST_KNOWN_OVERALL_STATE, ReplicationOverallState.RUNNING.name)))
    }
}
