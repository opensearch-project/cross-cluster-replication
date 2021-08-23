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

package org.opensearch.replication.task

import org.opensearch.replication.MultiClusterAnnotations
import org.opensearch.replication.MultiClusterRestTestCase
import org.opensearch.replication.StartReplicationRequest
import org.opensearch.replication.`validate status due index task cancellation`
import org.opensearch.replication.`validate status due shard task cancellation`
import org.opensearch.replication.replicationStatus
import org.opensearch.replication.startReplication
import org.opensearch.replication.stopReplication
import org.opensearch.replication.getIndexReplicationTask
import org.opensearch.replication.getShardReplicationTasks
import org.assertj.core.api.Assertions.assertThat
import org.junit.Assert
import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.client.tasks.CancelTasksRequest
import org.opensearch.client.tasks.TaskId
import org.opensearch.common.settings.Settings
import java.util.Collections


const val LEADER = "leaderCluster"
const val FOLLOWER = "followCluster"
const val leaderIndexName = "leader_index"
const val followerIndexName = "follower_index"

@MultiClusterAnnotations.ClusterConfigurations(
    MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
    MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)
class TaskCancellationIT : MultiClusterRestTestCase() {
    fun `test user triggering cancel on a shard task`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val primaryShards = 3

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(
                CreateIndexRequest(leaderIndexName).settings(Settings.builder().put("index.number_of_shards", primaryShards).build()),
                RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))
            // Wait for Shard tasks to come up.
            var tasks = Collections.emptyList<String>()
            assertBusy {
                tasks = followerClient.getShardReplicationTasks(followerIndexName)
                Assert.assertEquals(tasks.size, primaryShards)
            }

            // Cancel one shard task
            val cancelTasksRequest = CancelTasksRequest.Builder().withTaskId(TaskId(tasks[0])).
                withWaitForCompletion(true).build()
            followerClient.tasks().cancel(cancelTasksRequest, RequestOptions.DEFAULT)

            // Verify that replication is continuing and the shards tasks are up and running
            assertBusy {
                Assert.assertEquals(followerClient.getShardReplicationTasks(followerIndexName).size, primaryShards)
                assertThat(followerClient.getIndexReplicationTask(followerIndexName).isNotBlank()).isTrue()
                `validate status due shard task cancellation`(followerClient.replicationStatus(followerIndexName))
            }
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test user triggering cancel on an index task`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))
            // Wait for Shard tasks to come up.
            assertBusy {
                assertThat(followerClient.getShardReplicationTasks(followerIndexName).isEmpty()).isEqualTo(false)
            }

            // Cancel the index replication task
            var task = followerClient.getIndexReplicationTask(followerIndexName)
            assertThat(task.isNullOrBlank()).isFalse()
            val cancelTasksRequest = CancelTasksRequest.Builder().withTaskId(TaskId(task)).
            withWaitForCompletion(true).build()
            followerClient.tasks().cancel(cancelTasksRequest, RequestOptions.DEFAULT)

            // Verify that replication has paused.
            assertBusy {
                assertThat(followerClient.getShardReplicationTasks(followerIndexName).isEmpty()).isTrue()
                assertThat(followerClient.getIndexReplicationTask(followerIndexName).isNullOrBlank()).isTrue()
                `validate status due index task cancellation`(followerClient.replicationStatus(followerIndexName))
            }
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }
}
