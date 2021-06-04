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

package org.opensearch.replication.metadata

import org.assertj.core.api.Assertions.assertThat
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.XContentTestUtils

class ReplicationMetadataTests : OpenSearchTestCase() {

    companion object {
        val TEST_PATTERN = AutoFollowPattern("2020 logs", "logs-2020*")
        val REMOTE_CLUSTER_ALIAS = "leader"
        const val INDEX = "index"
        const val REMOTE_INDEX = "remoteIndex"
    }
    fun `test add autofollow pattern and diff`() {
        val expected = ReplicationMetadata.EMPTY.addPattern(REMOTE_CLUSTER_ALIAS, TEST_PATTERN)
        assertEquals(TEST_PATTERN, expected.autoFollowPatterns.getValue(REMOTE_CLUSTER_ALIAS).getValue("2020 logs"))

        val diff = expected.diff(ReplicationMetadata.EMPTY)
        val actual = diff.apply(ReplicationMetadata.EMPTY)
        assertEquals(expected, actual)
    }

    fun `test remove autofollow pattern and diff`() {
        val metadata = ReplicationMetadata.EMPTY.addPattern(REMOTE_CLUSTER_ALIAS, TEST_PATTERN)
        val expected = metadata.removePattern(REMOTE_CLUSTER_ALIAS, TEST_PATTERN.name)
        assertThat(expected.autoFollowPatterns[REMOTE_CLUSTER_ALIAS]).isEmpty()

        val diff = expected.diff(metadata)
        val actual = diff.apply(metadata)
        assertEquals(expected, actual)
    }

    fun `test add replicated index and diff`() {
        val expected = ReplicationMetadata.EMPTY.addIndex(REMOTE_CLUSTER_ALIAS, INDEX, REMOTE_INDEX)
        assertEquals(REMOTE_INDEX, expected.replicatedIndices.getValue(REMOTE_CLUSTER_ALIAS).getValue(INDEX))

        val diff = expected.diff(ReplicationMetadata.EMPTY)
        val actual = diff.apply(ReplicationMetadata.EMPTY)
        assertEquals(expected, actual)
    }

    fun `test remove replicated index and diff`() {
        val metadata = ReplicationMetadata.EMPTY.addIndex(REMOTE_CLUSTER_ALIAS, INDEX, REMOTE_INDEX)
        val expected = metadata.removeIndex(REMOTE_CLUSTER_ALIAS, INDEX)
        assertThat(expected.replicatedIndices[REMOTE_CLUSTER_ALIAS]).isEmpty()

        val diff = expected.diff(metadata)
        val actual = diff.apply(metadata)
        assertEquals(expected, actual)
    }

    fun `test remove cluster`() {
        val metadata = ReplicationMetadata.EMPTY.addIndex(REMOTE_CLUSTER_ALIAS, INDEX, REMOTE_INDEX)
            .addPattern(REMOTE_CLUSTER_ALIAS, TEST_PATTERN)
        assertThat(metadata.autoFollowPatterns).containsKey(REMOTE_CLUSTER_ALIAS)
        assertThat(metadata.replicatedIndices).containsKey(REMOTE_CLUSTER_ALIAS)

        val removed = metadata.removeRemoteCluster(REMOTE_CLUSTER_ALIAS)
        assertThat(removed.autoFollowPatterns).doesNotContainKey(REMOTE_CLUSTER_ALIAS)
        assertThat(removed.replicatedIndices).doesNotContainKey(REMOTE_CLUSTER_ALIAS)
    }

    fun `test serialization`() {
        val expected = ReplicationMetadata.EMPTY
            .addPattern(REMOTE_CLUSTER_ALIAS, TEST_PATTERN)
            .addIndex(REMOTE_CLUSTER_ALIAS, INDEX, REMOTE_INDEX)
        val output = BytesStreamOutput()
        expected.writeTo(output)
        val deserialized = ReplicationMetadata(output.bytes().streamInput())
        assertEquals(expected, deserialized)
    }

    fun `test json serialization`() {
        val expected = ReplicationMetadata.EMPTY
            .addPattern(REMOTE_CLUSTER_ALIAS, TEST_PATTERN)
            .addIndex(REMOTE_CLUSTER_ALIAS, INDEX, REMOTE_INDEX)

        val actual = XContentTestUtils.convertToMap(expected)
        assertThat(actual).containsKey("auto_follow_patterns")
        assertThat(actual["auto_follow_patterns"] as Map<String, Map<String, String?>>)
            .containsEntry(REMOTE_CLUSTER_ALIAS, mapOf(TEST_PATTERN.name to TEST_PATTERN.pattern))
    }
}