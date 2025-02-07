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

package org.opensearch.replication

import org.opensearch.replication.MultiClusterAnnotations.ClusterConfiguration
import org.opensearch.replication.MultiClusterAnnotations.ClusterConfigurations
import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.DocWriteResponse.Result
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.get.GetRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.common.xcontent.XContentType
import org.opensearch.common.CheckedRunnable
import org.opensearch.test.OpenSearchTestCase.assertBusy
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.index.IndexSettings
import org.opensearch.common.settings.Settings
import org.opensearch.client.indices.PutMappingRequest
import org.junit.Assert
import java.util.Locale
import java.util.concurrent.TimeUnit

const val LEADER = "leaderCluster"
const val FOLL = "followCluster"

@ClusterConfigurations(
    ClusterConfiguration(clusterName = LEADER),
    ClusterConfiguration(clusterName = FOLL)
)
class BasicReplicationIT : MultiClusterRestTestCase() {
    private val leaderIndexName = "leader_index"
    private val followerIndexName = "follower_index"

    fun `test empty index replication`() {
        val follower = getClientForCluster(FOLL)
        val leader = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLL, LEADER)
        val leaderIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT)
        val followerIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT)
        // Create an empty index on the leader and trigger replication on it
        val createIndexResponse = leader.indices().create(CreateIndexRequest(leaderIndex), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        follower.startReplication(StartReplicationRequest("source", leaderIndex, followerIndex), waitForRestore=true)
        val source = mapOf("name" to randomAlphaOfLength(20), "age" to randomInt().toString())
        var response = leader.index(IndexRequest(leaderIndex).id("1").source(source), RequestOptions.DEFAULT)
        assertThat(response.result).isEqualTo(Result.CREATED)
        assertBusy({
            val getResponse = follower.get(GetRequest(followerIndex, "1"), RequestOptions.DEFAULT)
            assertThat(getResponse.isExists).isTrue()
            assertThat(getResponse.sourceAsMap).isEqualTo(source)
        }, 60L, TimeUnit.SECONDS)
        // Ensure force merge on leader doesn't impact replication
        for (i in 2..5) {
            response = leader.index(IndexRequest(leaderIndex).id("$i").source(source), RequestOptions.DEFAULT)
            assertThat(response.result).isEqualTo(Result.CREATED)
        }
        leader.indices().forcemerge(ForceMergeRequest(leaderIndex), RequestOptions.DEFAULT)
        for (i in 6..10) {
            response = leader.index(IndexRequest(leaderIndex).id("$i").source(source), RequestOptions.DEFAULT)
            assertThat(response.result).isEqualTo(Result.CREATED)
        }
        assertBusy({
            for (i in 2..10) {
                val getResponse = follower.get(GetRequest(followerIndex, "$i"), RequestOptions.DEFAULT)
                assertThat(getResponse.isExists).isTrue()
                assertThat(getResponse.sourceAsMap).isEqualTo(source)
            }
        }, 60L, TimeUnit.SECONDS)
        // Force merge on follower however isn't allowed due to WRITE block
        Assertions.assertThatThrownBy {
            follower.indices().forcemerge(ForceMergeRequest(followerIndex), RequestOptions.DEFAULT)
        }.isInstanceOf(OpenSearchStatusException::class.java)
            .hasMessage("OpenSearch exception [type=cluster_block_exception, reason=index [$followerIndex] " +
                    "blocked by: [FORBIDDEN/1000/index read-only(cross-cluster-replication)];]")
    }

    fun `test knn index replication`() {


        val followerClient = getClientForCluster(FOLL)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLL, LEADER)
        val leaderIndexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT)
        val followerIndexNameInitial = randomAlphaOfLength(10).toLowerCase(Locale.ROOT)
        val followerIndexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT)
        val settings: Settings = Settings.builder()
            .put("index.knn", true)
            .build()
        val KNN_INDEX_MAPPING = "{\"properties\":{\"my_vector1\":{\"type\":\"knn_vector\",\"dimension\":2},\"my_vector2\":{\"type\":\"knn_vector\",\"dimension\":4}}}"
        // create knn-index on leader cluster
        try {
            val createIndexResponse = leaderClient.indices().create(
                CreateIndexRequest(leaderIndexName).settings(settings)
                    .mapping(KNN_INDEX_MAPPING, XContentType.JSON), RequestOptions.DEFAULT
            )
            assertThat(createIndexResponse.isAcknowledged).isTrue()
        } catch (e: Exception){
            //index creation will fail if Knn plugin is not installed
            assumeNoException("Could not create Knn index on leader cluster", e)
        }
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName), waitForRestore=true)
        // Create document
        var source = mapOf("my_vector1" to listOf(2.5,3.5) , "price" to 7.1)
        var response = leaderClient.index(IndexRequest(leaderIndexName).id("1").source(source), RequestOptions.DEFAULT)
        assertThat(response.result).withFailMessage("Failed to create leader data").isEqualTo(Result.CREATED)
        assertBusy({
            val getResponse = followerClient.get(GetRequest(followerIndexName, "1"), RequestOptions.DEFAULT)
            assertThat(getResponse.isExists).isTrue()
            assertThat(getResponse.sourceAsMap).isEqualTo(source)
        }, 60L, TimeUnit.SECONDS)

        // Update document
        source = mapOf("my_vector1" to listOf(3.5,4.5) , "price" to 12.9)
        response = leaderClient.index(IndexRequest(leaderIndexName).id("1").source(source), RequestOptions.DEFAULT)
        assertThat(response.result).withFailMessage("Failed to update leader data").isEqualTo(Result.UPDATED)
        assertBusy({
            val getResponse = followerClient.get(GetRequest(followerIndexName, "1"), RequestOptions.DEFAULT)
            assertThat(getResponse.isExists).isTrue()
            assertThat(getResponse.sourceAsMap).isEqualTo(source)
        },60L, TimeUnit.SECONDS)
        val KNN_INDEX_MAPPING1 = "{\"properties\":{\"my_vector1\":{\"type\":\"knn_vector\",\"dimension\":2},\"my_vector2\":{\"type\":\"knn_vector\",\"dimension\":4},\"my_vector3\":{\"type\":\"knn_vector\",\"dimension\":4}}}"
        val updateIndexResponse = leaderClient.indices().putMapping(
            PutMappingRequest(leaderIndexName).source(KNN_INDEX_MAPPING1, XContentType.JSON) , RequestOptions.DEFAULT
        )
        source = mapOf("my_vector3" to listOf(3.1,4.5,5.7,8.9) , "price" to 17.9)
        response = leaderClient.index(IndexRequest(leaderIndexName).id("2").source(source), RequestOptions.DEFAULT)
        assertThat(response.result).withFailMessage("Failed to update leader data").isEqualTo(Result.CREATED)
        assertBusy({
            val getResponse = followerClient.get(GetRequest(followerIndexName, "2"), RequestOptions.DEFAULT)
            assertThat(getResponse.isExists).isTrue()
            assertThat(getResponse.sourceAsMap).isEqualTo(source)
        },60L, TimeUnit.SECONDS)
        assertThat(updateIndexResponse.isAcknowledged).isTrue()
        // Delete document
        val deleteResponse = leaderClient.delete(DeleteRequest(leaderIndexName).id("1"), RequestOptions.DEFAULT)
        assertThat(deleteResponse.result).withFailMessage("Failed to delete leader data").isEqualTo(Result.DELETED)
        assertBusy({
            val getResponse = followerClient.get(GetRequest(followerIndexName, "1"), RequestOptions.DEFAULT)
            assertThat(getResponse.isExists).isFalse()
        }, 60L, TimeUnit.SECONDS)
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
    }

    fun `test that index operations are replayed to follower during replication`() {
        val followerClient = getClientForCluster(FOLL)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLL, LEADER)
        val leaderIndexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT)
        val followerIndexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT)
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName), waitForRestore=true)
        // Create document
        var source = mapOf("name" to randomAlphaOfLength(20), "age" to randomInt().toString())
        var response = leaderClient.index(IndexRequest(leaderIndexName).id("1").source(source), RequestOptions.DEFAULT)
        assertThat(response.result).withFailMessage("Failed to create leader data").isEqualTo(Result.CREATED)
        assertBusy({
            val getResponse = followerClient.get(GetRequest(followerIndexName, "1"), RequestOptions.DEFAULT)
            assertThat(getResponse.isExists).isTrue()
            assertThat(getResponse.sourceAsMap).isEqualTo(source)
        }, 60L, TimeUnit.SECONDS)
        // Update document
        source = mapOf("name" to randomAlphaOfLength(20), "age" to randomInt().toString())
        response = leaderClient.index(IndexRequest(leaderIndexName).id("1").source(source), RequestOptions.DEFAULT)
        assertThat(response.result).withFailMessage("Failed to update leader data").isEqualTo(Result.UPDATED)
        assertBusy({
            val getResponse = followerClient.get(GetRequest(followerIndexName, "1"), RequestOptions.DEFAULT)
            assertThat(getResponse.isExists).isTrue()
            assertThat(getResponse.sourceAsMap).isEqualTo(source)
        },60L, TimeUnit.SECONDS)
        // Delete document
        val deleteResponse = leaderClient.delete(DeleteRequest(leaderIndexName).id("1"), RequestOptions.DEFAULT)
        assertThat(deleteResponse.result).withFailMessage("Failed to delete leader data").isEqualTo(Result.DELETED)
        assertBusy({
            val getResponse = followerClient.get(GetRequest(followerIndexName, "1"), RequestOptions.DEFAULT)
            assertThat(getResponse.isExists).isFalse()
        }, 60L, TimeUnit.SECONDS)
    }
}
