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
import org.opensearch.replication.`validate aggregated paused status response`
import org.opensearch.replication.`validate paused status response`
import org.opensearch.replication.pauseReplication
import org.opensearch.replication.replicationStatus
import org.opensearch.replication.resumeReplication
import org.opensearch.replication.startReplication
import org.opensearch.replication.stopReplication
import org.opensearch.replication.updateReplication
import org.opensearch.replication.getShardReplicationTasks
import org.opensearch.replication.`validate paused status response due to leader index deleted`
import org.opensearch.replication.`validate status syncing response`
import org.apache.hc.core5.http.io.entity.EntityUtils
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.ResponseException
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.client.indices.GetIndexRequest
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.index.mapper.MapperService
import java.util.concurrent.TimeUnit


@MultiClusterAnnotations.ClusterConfigurations(
        MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
        MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)
class PauseReplicationIT: MultiClusterRestTestCase() {
    private val leaderIndexName = "leader_index"

    fun `test pause replication in following state and empty index`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "pause_index_follow_state"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName), waitForRestore = true)

            val myReason = "I want to pause!"

            /* At this point, the follower cluster should be in FOLLOWING state. Next, we pause replication
            and verify the same
             */
            followerClient.pauseReplication(followerIndexName, myReason)
            // Since, we were still in FOLLOWING phase when pause was called, the index
            // in follower index should not have been deleted in follower cluster
            assertBusy {
                assertThat(followerClient.indices()
                        .exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT))
                        .isEqualTo(true)
            }

            val statusResp = followerClient.replicationStatus(followerIndexName)
            `validate paused status response`(statusResp, myReason)

            var settings = Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .build()

            followerClient.updateReplication( followerIndexName, settings)
            followerClient.resumeReplication(followerIndexName)
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
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
        val followerIndexName = "pause_index_restore_state"
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
        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName),
                    TimeValue.timeValueSeconds(10),
                    false)
            //Given the size of index, the replication should be in RESTORING phase at this point
            assertThatThrownBy {
                followerClient.pauseReplication(followerIndexName)
            }.isInstanceOf(ResponseException::class.java)
                    .hasMessageContaining("Index is in restore phase currently for index: ${followerIndexName}")
            // wait for the shard tasks to be up as the replication block is added before adding shard replication tasks
            // During intermittent test failures, stop replication under finally block executes before this without removing
            // replication block (even though next call to _stop replication API can succeed in removing this block).
            assertBusy({
                assertTrue(followerClient.getShardReplicationTasks(followerIndexName).isNotEmpty())
            }, 30L, TimeUnit.SECONDS)
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test pause without replication in progress`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val followerIndexName = "pause_index_no_repl"
        //ToDo : Using followerIndex interferes with other test. Is wipeIndicesFromCluster not working ?
        var randomIndex = "random"
        val createIndexResponse = followerClient.indices().create(CreateIndexRequest(randomIndex),
                RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        assertThatThrownBy {
            followerClient.pauseReplication(randomIndex)
            var statusResp = followerClient.replicationStatus(followerIndexName)
            `validate paused status response`(statusResp)
            statusResp = followerClient.replicationStatus(followerIndexName,false)
            `validate aggregated paused status response`(statusResp)
        }.isInstanceOf(ResponseException::class.java)
                .hasMessageContaining("No replication in progress for index:$randomIndex")
    }

    fun `test pause replication and stop replication`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "pause_index_with_stop"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName), waitForRestore = true)

            /* At this point, the follower cluster should be in FOLLOWING state. Next, we pause replication
            and verify the same
             */
            followerClient.pauseReplication(followerIndexName)
            var statusResp = followerClient.replicationStatus(followerIndexName)
            `validate paused status response`(statusResp)
            statusResp = followerClient.replicationStatus(followerIndexName,false)
            `validate aggregated paused status response`(statusResp)
            // Since, we were still in FOLLOWING phase when pause was called, the index
            // in follower index should not have been deleted in follower cluster
            assertBusy {
                assertThat(followerClient.indices()
                        .exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT))
                        .isEqualTo(true)
            }
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test pause replication when leader cluster is unavailable`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val followerIndexName = "pause_index_leader_down"
        try {
            val leaderClient = getClientForCluster(LEADER)
            createConnectionBetweenClusters(FOLLOWER, LEADER)
            val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
            assertThat(createIndexResponse.isAcknowledged).isTrue()
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName),
                    waitForRestore = true)
            // Need to wait till index blocks appear into state
            assertBusy({
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

            followerClient.pauseReplication(followerIndexName)

            val statusResp = followerClient.replicationStatus(followerIndexName)
            `validate paused status response`(statusResp)

        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test auto pause of index replication when leader index is unavailable`() {
        val followerIndexName1 = "auto_pause_index"
        val leaderIndexName1 = "leader1"
        val followerIndexName2 = "no_auto_pause_index"
        val leaderIndexName2 = "leader2"
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        try {
            createConnectionBetweenClusters(FOLLOWER, LEADER)
            var createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName1), RequestOptions.DEFAULT)
            assertThat(createIndexResponse.isAcknowledged).isTrue()
            createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName2), RequestOptions.DEFAULT)
            assertThat(createIndexResponse.isAcknowledged).isTrue()

            // For followerIndexName1
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName1,
                followerIndexName1), waitForRestore = true)

            // For followerIndexName2
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName2,
                followerIndexName2), waitForRestore = true)

            val deleteResponse = leaderClient.indices().delete(DeleteIndexRequest(leaderIndexName1), RequestOptions.DEFAULT)
            assertThat(deleteResponse.isAcknowledged)

            // followerIndexName1 -> autopause
            assertBusy({
                var statusResp = followerClient.replicationStatus(followerIndexName1)
                assertThat(statusResp.containsKey("status"))
                assertThat(statusResp.containsKey("reason"))
                `validate paused status response due to leader index deleted`(statusResp)
            }, 30, TimeUnit.SECONDS)

            // followerIndexName2 -> Syncing state
            assertBusy({
                var statusResp = followerClient.replicationStatus(followerIndexName2)
                `validate status syncing response`(statusResp)
            }, 30, TimeUnit.SECONDS)

        } finally {
            followerClient.stopReplication(followerIndexName2)
            followerClient.stopReplication(followerIndexName1)
        }
    }
}
