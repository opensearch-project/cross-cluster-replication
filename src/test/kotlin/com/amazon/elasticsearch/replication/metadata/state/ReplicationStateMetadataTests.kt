/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.elasticsearch.replication.metadata.state

import com.amazon.elasticsearch.replication.metadata.ReplicationOverallState
import org.assertj.core.api.Assertions.assertThat
import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.test.ESTestCase
import org.elasticsearch.test.XContentTestUtils

class ReplicationStateMetadataTests : ESTestCase() {

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