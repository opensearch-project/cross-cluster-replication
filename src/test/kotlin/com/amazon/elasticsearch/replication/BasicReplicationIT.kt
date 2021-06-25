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

package com.amazon.elasticsearch.replication

import com.amazon.elasticsearch.replication.MultiClusterAnnotations.ClusterConfiguration
import com.amazon.elasticsearch.replication.MultiClusterAnnotations.ClusterConfigurations
import org.assertj.core.api.Assertions.assertThat
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.DocWriteResponse.Result
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.test.ESTestCase.assertBusy
import org.junit.Assert
import java.util.Locale

const val LEADER = "leaderCluster"
const val FOLL = "followCluster"

@ClusterConfigurations(
    ClusterConfiguration(clusterName = LEADER),
    ClusterConfiguration(clusterName = FOLL)
)
class BasicReplicationIT : MultiClusterRestTestCase() {

    fun `test empty index replication`() {
        val follower = getClientForCluster(FOLL)
        val leader = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLL, LEADER)

        val leaderIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT)
        val followerIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT)
        // Create an empty index on the leader and trigger replication on it
        val createIndexResponse = leader.indices().create(CreateIndexRequest(leaderIndex), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            follower.startReplication(StartReplicationRequest("source", leaderIndex, followerIndex), waitForRestore=true)

            val source = mapOf("name" to randomAlphaOfLength(20), "age" to randomInt().toString())
            val response = leader.index(IndexRequest(leaderIndex).id("1").source(source), RequestOptions.DEFAULT)
            assertThat(response.result).isEqualTo(Result.CREATED)

            assertBusy {
                val getResponse = follower.get(GetRequest(followerIndex, "1"), RequestOptions.DEFAULT)
                assertThat(getResponse.isExists).isTrue()
                assertThat(getResponse.sourceAsMap).isEqualTo(source)
            }
        } finally {
            follower.stopReplication(followerIndex)
        }
    }

    fun `test existing index replication`() {
        val follower = getClientForCluster(FOLL)
        val leader = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLL, LEADER)

        // Create an index with data before commencing replication
        val leaderIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT)
        val followerIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT)
        val source = mapOf("name" to randomAlphaOfLength(20), "age" to randomInt().toString())
        val response = leader.index(IndexRequest(leaderIndex).id("1").source(source), RequestOptions.DEFAULT)
        assertThat(response.result).withFailMessage("Failed to create leader data").isEqualTo(Result.CREATED)

        follower.startReplication(StartReplicationRequest("source", leaderIndex, followerIndex), waitForRestore=true)

        assertBusy {
            val getResponse = follower.get(GetRequest(followerIndex, "1"), RequestOptions.DEFAULT)
            assertThat(getResponse.isExists).isTrue()
            assertThat(getResponse.sourceAsMap).isEqualTo(source)
        }
        follower.stopReplication(followerIndex)
    }
}
