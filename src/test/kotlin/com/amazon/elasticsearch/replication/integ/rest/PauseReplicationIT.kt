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

import com.amazon.elasticsearch.replication.*
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



@MultiClusterAnnotations.ClusterConfigurations(
        MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
        MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)
class PauseReplicationIT: MultiClusterRestTestCase() {
    private val leaderIndexName = "leader_index"
    private val followerIndexName = "paused_index"

    fun `test pause replication in following state and empty index`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName), waitForRestore = true)

        /* At this point, the follower cluster should be in FOLLOWING state. Next, we pause replication
        and verify the same
         */
        followerClient.pauseReplication(followerIndexName)
        // Since, we were still in FOLLOWING phase when pause was called, the index
        // in follower index should not have been deleted in follower cluster
        assertBusy {
            assertThat(followerClient.indices()
                    .exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT))
                    .isEqualTo(true)
        }

        followerClient.resumeReplication(followerIndexName)
    }

    fun `test pause replication in restoring state with multiple shards`() {
        val settings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 20)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.key, Long.MAX_VALUE)
                .build()
        testPauseReplicationInRestoringState(settings, 5000, 1000, 1000)
    }

    private fun testPauseReplicationInRestoringState(settings: Settings,
                                                     nFields: Int,
                                                     fieldLength: Int,
                                                     stepSize: Int) {
        logger.info("""Testing pause replication in restoring state with params: 
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
        assertThatThrownBy {
            followerClient.pauseReplication(followerIndexName)
        }.isInstanceOf(ResponseException::class.java)
                .hasMessageContaining("Index is in bootstrap phase currently for index:${followerIndexName}")
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

    fun `test pause without replication in progress`() {
        val followerClient = getClientForCluster(FOLLOWER)
        //ToDo : Using followerIndex interferes with other test. Is wipeIndicesFromCluster not working ?
        var randomIndex = "random"
        val createIndexResponse = followerClient.indices().create(CreateIndexRequest(randomIndex),
                RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        assertThatThrownBy {
            followerClient.pauseReplication(randomIndex)
        }.isInstanceOf(ResponseException::class.java)
                .hasMessageContaining("No replication in progress for index:$randomIndex")
    }

    fun `test pause replication and stop replication`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName), waitForRestore = true)

        /* At this point, the follower cluster should be in FOLLOWING state. Next, we pause replication
        and verify the same
         */
        followerClient.pauseReplication(followerIndexName)
        // Since, we were still in FOLLOWING phase when pause was called, the index
        // in follower index should not have been deleted in follower cluster
        assertBusy {
            assertThat(followerClient.indices()
                    .exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT))
                    .isEqualTo(true)
        }

        followerClient.stopReplication(followerIndexName)
    }

}
