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

package org.opensearch.replication.integ.rest

import org.opensearch.replication.IndexUtil
import org.opensearch.replication.MultiClusterAnnotations
import org.opensearch.replication.MultiClusterRestTestCase
import org.opensearch.replication.StartReplicationRequest
import org.opensearch.replication.startReplication
import org.opensearch.replication.stopReplication
import org.apache.http.util.EntityUtils
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.ResponseException
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.client.indices.GetIndexRequest
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.index.mapper.MapperService
import org.opensearch.test.OpenSearchTestCase.assertBusy
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
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName), waitForRestore = true)

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
        IndexUtil.fillIndex(leaderClient, leaderIndexName, nFields, fieldLength, stepSize)
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

    @AwaitsFix(bugUrl = "")
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
        }.isInstanceOf(OpenSearchStatusException::class.java)
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
            followerClient.stopReplication("no_index")
        }.isInstanceOf(ResponseException::class.java)
                .hasMessageContaining("No replication in progress for index:no_index")
    }

    fun `test stop replication when leader cluster is unavailable`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName),
            waitForRestore = true)
        // Need to wait till index blocks appear into state
        assertBusy ({
            val clusterBlocksResponse = followerClient.lowLevelClient.performRequest(Request("GET", "/_cluster/state/blocks"))
            val clusterResponseString = EntityUtils.toString(clusterBlocksResponse.entity)
            assertThat(clusterResponseString.contains("cross-cluster-replication"))
                .withFailMessage("Cant find replication block after starting replication")
                .isTrue()
        }, 10, TimeUnit.SECONDS)

        // setting an invalid seed so that leader cluster is unavailable
        val settings: Settings = Settings.builder()
            .putList("cluster.remote.source.seeds", "127.0.0.1:9305")
            .build()
        val updateSettingsRequest = ClusterUpdateSettingsRequest()
        updateSettingsRequest.persistentSettings(settings)
        followerClient.cluster().putSettings(updateSettingsRequest, RequestOptions.DEFAULT)

        followerClient.stopReplication(followerIndexName)
        val sourceMap = mapOf("name" to randomAlphaOfLength(5))
        followerClient.index(IndexRequest(followerIndexName).id("2").source(sourceMap), RequestOptions.DEFAULT)
    }
}
