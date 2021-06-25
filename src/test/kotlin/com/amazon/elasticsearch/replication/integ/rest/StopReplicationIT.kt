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
import com.amazon.elasticsearch.replication.startReplication
import com.amazon.elasticsearch.replication.stopReplication
import org.apache.http.util.EntityUtils
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.DocWriteResponse
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.flush.FlushRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Request
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.ResponseException
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.mapper.MapperService
import org.elasticsearch.test.ESTestCase.assertBusy
import java.util.concurrent.TimeUnit


const val LEADER = "leaderCluster"
const val FOLLOWER = "followCluster"

@MultiClusterAnnotations.ClusterConfigurations(
        MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
        MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)
class StopReplicationIT: MultiClusterRestTestCase() {
    private val leaderIndexName = "leader_index"
    private val followerIndexName = "follower_index"

    fun `test stop replication in following state and empty index`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))

        /* At this point, the follower cluster should be in FOLLOWING state. Next, we stop replication
        and verify the same
         */
        followerClient.stopReplication(followerIndexName)
        // Since, we were still in FOLLOWING phase when stop was called, the index
        // in follower index should not have been deleted in follower cluster
        assertBusy {
            assertThat(followerClient.indices()
                    .exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT))
                    .isEqualTo(true)
        }
    }

    fun `test stop replication in restoring state with multiple shards`() {
        val settings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 20)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.key, Long.MAX_VALUE)
                .build()
        testStopReplicationInRestoringState(settings, 5000, 1000, 1000)
    }

    private fun testStopReplicationInRestoringState(settings: Settings,
                                                    nFields: Int,
                                                    fieldLength: Int,
                                                    stepSize: Int) {
        logger.info("""Testing stop replication in restoring state with params: 
            | shards:$settings[IndexMetadata.SETTING_NUMBER_OF_SHARDS]
            | nFields:$nFields
            | fieldLength:$fieldLength
            | stepSize:$stepSize 
            | """.trimMargin())
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName).settings(settings),
                RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        // Put a large amount of data into the index
        fillIndex(leaderClient, leaderIndexName, nFields, fieldLength, stepSize)
        assertBusy {
            assertThat(leaderClient.indices()
                    .exists(GetIndexRequest(leaderIndexName), RequestOptions.DEFAULT))
        }
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName),
                TimeValue.timeValueSeconds(10),
                false)
        //Given the size of index, the replication should be in RESTORING phase at this point
        followerClient.stopReplication(followerIndexName)
        // Since, we were still in RESTORING phase when stop was called, the index
        // in follower index should have been deleted in follower cluster
        assertBusy {
            assertThat(followerClient.indices()
                    .exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT))
                    .isEqualTo(false)
        }
    }

    /* What we want to test here is the there is that STOP replication
        is called while shard tasks were starting. Since we can't have this situation
        deterministically, we have a high number of shards and repeated tests. This is so that
        there is some shard task in follower index which which gets started after STOP api has closed
        existing shard tasks. This is how it was tested manually. */
    // TODO: Figure out a way without using @Repeat(iterations = 5)
    fun `test stop replication in restoring state while shards are starting`() {
        val settings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 50)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        testStopReplicationInRestoringState(settings, 5, 10, 5)
    }

    private fun fillIndex(clusterClient: RestHighLevelClient,
                          indexName : String,
                          nFields: Int,
                          fieldLength: Int,
                          stepSize: Int) {
        for (i in nFields downTo 1 step stepSize) {
            val sourceMap : MutableMap<String, String> = HashMap()
            for (j in stepSize downTo 1)
                sourceMap[(i-j).toString()] = randomAlphaOfLength(fieldLength)
            logger.info("Updating index with map of size:${sourceMap.size}")
            val indexResponse = clusterClient.index(IndexRequest(indexName).id(i.toString()).source(sourceMap), RequestOptions.DEFAULT)
            assertThat(indexResponse.result).isIn(DocWriteResponse.Result.CREATED, DocWriteResponse.Result.UPDATED)
        }
        //flush the index
        clusterClient.indices().flush(FlushRequest(indexName), RequestOptions.DEFAULT)
    }

    fun `test follower index unblocked after stop replication`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        val sourceMap = mapOf("name" to randomAlphaOfLength(5))
        leaderClient.index(IndexRequest(leaderIndexName).id("1").source(sourceMap), RequestOptions.DEFAULT)
        // Need to set waitForRestore=true as the cluster blocks are added only
        // after restore is completed.
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName),
                waitForRestore = true)
        // Need to wait till index blocks appear into state
        assertBusy ({
            val clusterBlocksResponse = followerClient.lowLevelClient.performRequest(Request("GET", "/_cluster/state/blocks"))
            val clusterResponseString = EntityUtils.toString(clusterBlocksResponse.entity)
            assertThat(clusterResponseString.contains("cross-cluster-replication"))
                    .withFailMessage("Cant find replication block afer starting replication")
                    .isTrue()
        }, 10, TimeUnit.SECONDS)

        assertThatThrownBy {
            followerClient.index(IndexRequest(followerIndexName).id("blocked").source(sourceMap), RequestOptions
                .DEFAULT)
        }.isInstanceOf(ElasticsearchStatusException::class.java)
                .hasMessage("Elasticsearch exception [type=cluster_block_exception, reason=index [$followerIndexName] " +
                        "blocked by: [FORBIDDEN/1000/index read-only(cross-cluster-replication)];]")

        //Stop replication and verify that index is not blocked any more
        followerClient.stopReplication(followerIndexName)
        //Following line shouldn't throw any exception
        followerClient.index(IndexRequest(followerIndexName).id("2").source(sourceMap), RequestOptions.DEFAULT)
    }

    fun `test stop without replication in progress`() {
        val followerClient = getClientForCluster(FOLLOWER)
        assertThatThrownBy {
            followerClient.stopReplication(followerIndexName)
        }.isInstanceOf(ResponseException::class.java)
                .hasMessageContaining("No replication in progress for index:follower_index")
    }
}
