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
import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.client.tasks.CancelTasksRequest
import org.opensearch.client.tasks.TaskId
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

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))
            // Wait for Shard tasks to come up.
            var tasks = Collections.emptyList<String>()
            assertBusy {
                tasks = followerClient.getShardReplicationTasks(followerIndexName)
                assertThat(tasks.isEmpty()).isEqualTo(false)
            }

            // Cancel one shard task
            val cancelTasksRequest = CancelTasksRequest.Builder().withTaskId(TaskId(tasks[0])).
                withWaitForCompletion(true).build()
            followerClient.tasks().cancel(cancelTasksRequest, RequestOptions.DEFAULT)

            // Verify that replication has paused.
            assertBusy {
                assertThat(followerClient.getShardReplicationTasks(followerIndexName).isEmpty()).isTrue()
                assertThat(followerClient.getIndexReplicationTask(followerIndexName).isNullOrBlank()).isTrue()
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
