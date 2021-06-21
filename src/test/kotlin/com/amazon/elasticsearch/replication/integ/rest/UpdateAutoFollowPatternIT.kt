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

package com.amazon.elasticsearch.replication.integ.rest

import com.amazon.elasticsearch.replication.MultiClusterAnnotations
import com.amazon.elasticsearch.replication.MultiClusterRestTestCase
import com.amazon.elasticsearch.replication.StartReplicationRequest
import com.amazon.elasticsearch.replication.deleteAutoFollowPattern
import com.amazon.elasticsearch.replication.startReplication
import com.amazon.elasticsearch.replication.stopReplication
import com.amazon.elasticsearch.replication.task.autofollow.AutoFollowExecutor
import com.amazon.elasticsearch.replication.task.index.IndexReplicationExecutor
import com.amazon.elasticsearch.replication.updateAutoFollowPattern
import org.apache.http.HttpStatus
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.assertj.core.api.Assertions
import org.elasticsearch.client.Request
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.ResponseException
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.tasks.TaskInfo
import java.util.Locale

import java.util.concurrent.TimeUnit


@MultiClusterAnnotations.ClusterConfigurations(
        MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
        MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)
class UpdateAutoFollowPatternIT: MultiClusterRestTestCase() {
    private val indexPrefix = "leader_index_"
    private val indexPattern = "leader_index*"
    private val indexPatternName = "test_pattern"
    private val connectionAlias = "test_conn"

    fun `test auto follow pattern`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val leaderIndexName = createRandomIndex(leaderClient)
        var leaderIndexNameNew = ""
        createConnectionBetweenClusters(FOLLOWER, LEADER, connectionAlias)

        try {
            followerClient.updateAutoFollowPattern(connectionAlias, indexPatternName, indexPattern)

            // Verify that existing index matching the pattern are replicated.
            assertBusy ({
                Assertions.assertThat(followerClient.indices()
                        .exists(GetIndexRequest(leaderIndexName), RequestOptions.DEFAULT))
                        .isEqualTo(true)
            }, 30, TimeUnit.SECONDS)
            Assertions.assertThat(getAutoFollowTasks(FOLLOWER).size).isEqualTo(1)

            leaderIndexNameNew = createRandomIndex(leaderClient)
            // Verify that newly created index on leader which match the pattern are also replicated.
            assertBusy ({
                Assertions.assertThat(followerClient.indices()
                        .exists(GetIndexRequest(leaderIndexNameNew), RequestOptions.DEFAULT))
                        .isEqualTo(true)
            }, 30, TimeUnit.SECONDS)
        } finally {
            followerClient.deleteAutoFollowPattern(connectionAlias, indexPatternName)
            followerClient.stopReplication(leaderIndexName, false)
            followerClient.stopReplication(leaderIndexNameNew)
        }
    }

    fun `test auto follow shouldn't add already triggered index`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val leaderIndexName = createRandomIndex(leaderClient)
        createConnectionBetweenClusters(FOLLOWER, LEADER, connectionAlias)

        try {
            followerClient.startReplication(StartReplicationRequest(connectionAlias, leaderIndexName, leaderIndexName),
                    TimeValue.timeValueSeconds(10),true)

            assertBusy({
                Assertions.assertThat(followerClient.indices()
                        .exists(GetIndexRequest(leaderIndexName), RequestOptions.DEFAULT))
                        .isEqualTo(true)
            }, 30, TimeUnit.SECONDS)

            // Assert that there is no auto follow task & one index replication task
            Assertions.assertThat(getAutoFollowTasks(FOLLOWER).size).isEqualTo(0)
            Assertions.assertThat(getIndexReplicationTasks(FOLLOWER).size).isEqualTo(1)

            try {
                followerClient.updateAutoFollowPattern(connectionAlias, indexPatternName, indexPattern)

                // Assert that there is still only one index replication task
                Assertions.assertThat(getAutoFollowTasks(FOLLOWER).size).isEqualTo(1)
                Assertions.assertThat(getIndexReplicationTasks(FOLLOWER).size).isEqualTo(1)
            } finally {
                followerClient.deleteAutoFollowPattern(connectionAlias, indexPatternName)
            }
        } finally {
            followerClient.stopReplication(leaderIndexName)
        }
    }

    fun `test auto follow should fail if remote connection doesn't exist`() {
        val followerClient = getClientForCluster(FOLLOWER)
        // Call autofollow pattern without setting up remote connection.
        Assertions.assertThatThrownBy {
            followerClient.updateAutoFollowPattern(connectionAlias, indexPatternName, indexPattern)
        }.isInstanceOf(ResponseException::class.java)
                .hasMessageContaining("no such remote cluster")
    }

    fun `test removing autofollow pattern stop autofollow task`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER, connectionAlias)

        val leaderIndexName = createRandomIndex(leaderClient)

        try {
            followerClient.updateAutoFollowPattern(connectionAlias, indexPatternName, indexPattern)

            // Verify that existing index matching the pattern are replicated.
            assertBusy {
                Assertions.assertThat(followerClient.indices()
                        .exists(GetIndexRequest(leaderIndexName), RequestOptions.DEFAULT))
                        .isEqualTo(true)
            }

            Assertions.assertThat(getAutoFollowTasks(FOLLOWER).size).isEqualTo(1)
            Assertions.assertThat(getIndexReplicationTasks(FOLLOWER).size).isEqualTo(1)
        } finally {
            followerClient.deleteAutoFollowPattern(connectionAlias, indexPatternName)
        }

        // Verify that auto follow tasks is stopped but the shard replication task remains.
        assertBusy ({
            Assertions.assertThat(getAutoFollowTasks(FOLLOWER).size).isEqualTo(0)
        }, 30, TimeUnit.SECONDS)

        Assertions.assertThat(getIndexReplicationTasks(FOLLOWER).size).isEqualTo(1)
    }

    fun createRandomIndex(client: RestHighLevelClient): String {
        val indexName = indexPrefix + randomAlphaOfLength(6).toLowerCase(Locale.ROOT)
        val createIndexResponse = client.indices().create(CreateIndexRequest(indexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        assertBusy {
            Assertions.assertThat(client.indices()
                    .exists(GetIndexRequest(indexName), RequestOptions.DEFAULT))
                    .isEqualTo(true)
        }
        return indexName
    }
    fun getAutoFollowTasks(clusterName: String): List<TaskInfo> {
        return getReplicationTaskList(clusterName, AutoFollowExecutor.TASK_NAME + "*")
    }

    fun getIndexReplicationTasks(clusterName: String): List<TaskInfo> {
        return getReplicationTaskList(clusterName, IndexReplicationExecutor.TASK_NAME + "*")
    }

    fun createDummyConnection(fromClusterName: String, connectionName: String="source") {
        val fromCluster = getNamedCluster(fromClusterName)
        val persistentConnectionRequest = Request("PUT", "_cluster/settings")
        val toClusterDummyHostSeed = "localhost:65536"
        val entityAsString = """
                        {
                          "persistent": {
                             "cluster": {
                               "remote": {
                                 "$connectionName": {
                                   "seeds": [ "$toClusterDummyHostSeed" ]
                                 }
                               }
                             }
                          }
                        }""".trimMargin()

        persistentConnectionRequest.entity = NStringEntity(entityAsString, ContentType.APPLICATION_JSON)
        val persistentConnectionResponse = fromCluster.lowLevelClient.performRequest(persistentConnectionRequest)
        assertEquals(HttpStatus.SC_OK.toLong(), persistentConnectionResponse.statusLine.statusCode.toLong())
    }

}