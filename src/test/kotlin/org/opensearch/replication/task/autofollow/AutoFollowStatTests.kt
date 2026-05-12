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

package org.opensearch.replication.task.autofollow

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.test.OpenSearchTestCase
import org.assertj.core.api.Assertions.assertThat

class AutoFollowStatTests : OpenSearchTestCase() {

    fun testSerializationWithLeaderAlias() {
        val name = "test-autofollow-rule"
        val pattern = "test-pattern*"
        val leaderAlias = "test-leader-cluster"
        
        val stat = AutoFollowStat(name, pattern, leaderAlias)
        stat.successCount = 5
        stat.failCount = 2
        stat.failedLeaderCall = 1
        stat.lastExecutionTime = System.currentTimeMillis()
        stat.failedIndices.add("failed-index-1")
        stat.failedIndices.add("failed-index-2")

        // Serialize
        val output = BytesStreamOutput()
        stat.writeTo(output)

        // Deserialize
        val input = StreamInput.wrap(output.bytes().toBytesRef().bytes)
        val deserializedStat = AutoFollowStat(input)

        // Verify all fields
        assertThat(deserializedStat.name).isEqualTo(name)
        assertThat(deserializedStat.pattern).isEqualTo(pattern)
        assertThat(deserializedStat.leaderAlias).isEqualTo(leaderAlias)
        assertThat(deserializedStat.successCount).isEqualTo(5)
        assertThat(deserializedStat.failCount).isEqualTo(2)
        assertThat(deserializedStat.failedLeaderCall).isEqualTo(1)
        assertThat(deserializedStat.lastExecutionTime).isEqualTo(stat.lastExecutionTime)
        assertThat(deserializedStat.failedIndices).containsExactlyInAnyOrder("failed-index-1", "failed-index-2")
    }

    fun testBackwardCompatibilityWithoutLeaderAlias() {
        val name = "test-autofollow-rule"
        val pattern = "test-pattern*"
        
        // Simulate old version serialization (without leaderAlias)
        val output = BytesStreamOutput()
        output.writeString(name)
        output.writeString(pattern)
        // Skip leaderAlias - simulating old version
        output.writeLong(3) // failCount
        output.writeCollection(listOf("failed-1", "failed-2")) { out, value -> out.writeString(value) }
        output.writeLong(10) // successCount
        output.writeLong(1) // failedLeaderCall
        output.writeLong(System.currentTimeMillis()) // lastExecutionTime

        // Deserialize with new version
        val input = StreamInput.wrap(output.bytes().toBytesRef().bytes)
        val deserializedStat = AutoFollowStat(input)

        // Verify fields
        assertThat(deserializedStat.name).isEqualTo(name)
        assertThat(deserializedStat.pattern).isEqualTo(pattern)
        assertThat(deserializedStat.leaderAlias).isEqualTo("") // Should default to empty string
        assertThat(deserializedStat.failCount).isEqualTo(3)
        assertThat(deserializedStat.successCount).isEqualTo(10)
        assertThat(deserializedStat.failedLeaderCall).isEqualTo(1)
        assertThat(deserializedStat.failedIndices).containsExactlyInAnyOrder("failed-1", "failed-2")
    }

    fun testToXContentIncludesLeaderAlias() {
        val name = "test-autofollow-rule"
        val pattern = "test-pattern*"
        val leaderAlias = "test-leader-cluster"
        
        val stat = AutoFollowStat(name, pattern, leaderAlias)
        stat.successCount = 5
        stat.failCount = 2
        stat.failedLeaderCall = 1
        stat.lastExecutionTime = 1234567890L
        stat.failedIndices.add("failed-index-1")

        // Convert to XContent
        val builder = XContentFactory.jsonBuilder()
        stat.toXContent(builder, ToXContent.EMPTY_PARAMS)
        val json = builder.toString()

        // Verify JSON contains leader_alias
        assertThat(json).contains("\"name\":\"$name\"")
        assertThat(json).contains("\"pattern\":\"$pattern\"")
        assertThat(json).contains("\"leader_alias\":\"$leaderAlias\"")
        assertThat(json).contains("\"num_success_start_replication\":5")
        assertThat(json).contains("\"num_failed_start_replication\":2")
        assertThat(json).contains("\"num_failed_leader_calls\":1")
        assertThat(json).contains("\"last_execution_time\":1234567890")
        assertThat(json).contains("\"failed_indices\"")
        assertThat(json).contains("failed-index-1")
    }

    fun testEmptyLeaderAliasInXContent() {
        val name = "test-autofollow-rule"
        val pattern = "test-pattern*"
        val leaderAlias = ""
        
        val stat = AutoFollowStat(name, pattern, leaderAlias)

        // Convert to XContent
        val builder = XContentFactory.jsonBuilder()
        stat.toXContent(builder, ToXContent.EMPTY_PARAMS)
        val json = builder.toString()

        // Verify JSON contains empty leader_alias
        assertThat(json).contains("\"leader_alias\":\"\"")
    }

    fun testGetWriteableName() {
        val stat = AutoFollowStat("name", "pattern", "leader")
        assertThat(stat.writeableName).isEqualTo("autofollow_stat")
    }
}
