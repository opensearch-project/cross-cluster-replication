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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import org.assertj.core.api.Assertions.assertThat
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.persistent.PersistentTaskParams
import org.opensearch.persistent.PersistentTasksCustomMetadata
import org.opensearch.replication.task.index.IndexReplicationExecutor
import org.opensearch.replication.task.index.IndexReplicationParams
import org.opensearch.core.index.Index
import org.opensearch.test.OpenSearchTestCase

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class CleanupShardTasksUpdateTaskTests : OpenSearchTestCase() {

    fun testRemovesLegacyShardTasks() {
        val tasks = PersistentTasksCustomMetadata.builder()
        // Mix of legacy shard tasks and an index task. Cleanup should remove the shard tasks and keep the index task.
        tasks.addTask<PersistentTaskParams>(
            "replication:[follower-01][0]",
            CleanupShardTasksUpdateTask.LEGACY_SHARD_TASK_NAME,
            NoOpParams(CleanupShardTasksUpdateTask.LEGACY_SHARD_TASK_NAME, "alpha"),
            PersistentTasksCustomMetadata.Assignment("node-A", "test")
        )
        tasks.addTask<PersistentTaskParams>(
            "replication:[follower-01][1]",
            CleanupShardTasksUpdateTask.LEGACY_SHARD_TASK_NAME,
            NoOpParams(CleanupShardTasksUpdateTask.LEGACY_SHARD_TASK_NAME, "beta"),
            PersistentTasksCustomMetadata.Assignment("node-B", "test")
        )
        tasks.addTask<PersistentTaskParams>(
            "replication:index:follower-01",
            IndexReplicationExecutor.TASK_NAME,
            IndexReplicationParams("leader-cluster", Index("leader-01", "_na_"), "follower-01"),
            PersistentTasksCustomMetadata.Assignment("node-C", "test")
        )

        val before = ClusterState.builder(ClusterName("test"))
            .metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasks.build()))
            .build()

        val after = CleanupShardTasksUpdateTask().execute(before)
        val result = after.metadata().custom<PersistentTasksCustomMetadata>(PersistentTasksCustomMetadata.TYPE)

        assertThat(result.tasks()).hasSize(1)
        assertThat(result.tasks().first().taskName).isEqualTo(IndexReplicationExecutor.TASK_NAME)
        assertThat(result.tasks().first().id).isEqualTo("replication:index:follower-01")
    }

    fun testIdempotentWhenNoLegacyTasks() {
        val tasks = PersistentTasksCustomMetadata.builder()
        tasks.addTask<PersistentTaskParams>(
            "replication:index:follower-01",
            IndexReplicationExecutor.TASK_NAME,
            IndexReplicationParams("leader-cluster", Index("leader-01", "_na_"), "follower-01"),
            PersistentTasksCustomMetadata.Assignment("node-A", "test")
        )

        val before = ClusterState.builder(ClusterName("test"))
            .metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasks.build()))
            .build()

        val after = CleanupShardTasksUpdateTask().execute(before)

        // Idempotent: no legacy tasks present, so the task returns the input state unchanged.
        assertThat(after).isSameAs(before)
    }

    fun testNoPersistentTasksMetadata() {
        val before = ClusterState.builder(ClusterName("test")).metadata(Metadata.builder()).build()
        val after = CleanupShardTasksUpdateTask().execute(before)
        assertThat(after).isSameAs(before)
    }

    /**
     * Stand-in for legacy ShardReplicationParams used only to populate task entries in cluster state for the test.
     * The cleanup logic filters on taskName, so the params type is irrelevant — but PersistentTask requires *some*
     * PersistentTaskParams instance.
     */
    /**
     * The [PersistentTasksCustomMetadata.PersistentTask] constructor enforces
     * `params.writeableName == taskName`. The test params take the matching task name as a constructor argument
     * so we can build entries for both legacy shard tasks and the index task.
     */
    private class NoOpParams(private val taskName: String, private val tag: String) : PersistentTaskParams {
        override fun getWriteableName() = taskName
        override fun writeTo(out: org.opensearch.core.common.io.stream.StreamOutput) { out.writeString(tag) }
        override fun toXContent(builder: org.opensearch.core.xcontent.XContentBuilder, params: org.opensearch.core.xcontent.ToXContent.Params?) =
            builder.startObject().field("tag", tag).endObject()
        override fun getMinimalSupportedVersion(): org.opensearch.Version = org.opensearch.Version.V_2_0_0
    }
}
