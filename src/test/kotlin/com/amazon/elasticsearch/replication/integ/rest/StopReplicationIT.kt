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

import com.amazon.elasticsearch.replication.IndexUtil
import com.amazon.elasticsearch.replication.MultiClusterAnnotations
import com.amazon.elasticsearch.replication.MultiClusterRestTestCase
import com.amazon.elasticsearch.replication.StartReplicationRequest
import com.amazon.elasticsearch.replication.startReplication
import com.amazon.elasticsearch.replication.stopReplication
import com.amazon.elasticsearch.replication.replicationStatus
import com.amazon.elasticsearch.replication.getShardReplicationTasks
import com.amazon.elasticsearch.replication.`validate status syncing response`
import org.apache.http.util.EntityUtils
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.Assert
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Request
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.ResponseException
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.cluster.SnapshotsInProgress
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.mapper.MapperService
import org.elasticsearch.test.ESTestCase.assertBusy
import org.elasticsearch.common.Randomness
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

    fun `test stop replication when leader cluster is removed`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER, "source")
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
        // Remove leader cluster from settings
        val settings: Settings = Settings.builder()
                .putNull("cluster.remote.source.seeds")
                .build()
        val updateSettingsRequest = ClusterUpdateSettingsRequest()
        updateSettingsRequest.persistentSettings(settings)
        followerClient.cluster().putSettings(updateSettingsRequest, RequestOptions.DEFAULT)

        followerClient.stopReplication(followerIndexName)
        val sourceMap = mapOf("name" to randomAlphaOfLength(5))
        followerClient.index(IndexRequest(followerIndexName).id("2").source(sourceMap), RequestOptions.DEFAULT)
    }

    fun `test stop replication with stale replication settings at leader cluster`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER, "source")

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        val snapshotSuffix = Randomness.get().nextInt(1000).toString()

        try {
            followerClient.startReplication(
                    StartReplicationRequest("source", leaderIndexName, followerIndexName),
                    TimeValue.timeValueSeconds(10),
                    true
            )

            assertBusy({
                var statusResp = followerClient.replicationStatus(followerIndexName)
                `validate status syncing response`(statusResp)
                assertThat(followerClient.getShardReplicationTasks(followerIndexName)).isNotEmpty()
            }, 60, TimeUnit.SECONDS)

            // Trigger snapshot on the follower cluster
            val createSnapshotRequest = CreateSnapshotRequest(TestCluster.FS_SNAPSHOT_REPO, "test-$snapshotSuffix")
            createSnapshotRequest.waitForCompletion(true)
            followerClient.snapshot().create(createSnapshotRequest, RequestOptions.DEFAULT)

            assertBusy {
                var snapshotStatusResponse = followerClient.snapshot().status(SnapshotsStatusRequest(TestCluster.FS_SNAPSHOT_REPO,
                        arrayOf("test-$snapshotSuffix")), RequestOptions.DEFAULT)
                for (snapshotStatus in snapshotStatusResponse.snapshots) {
                    Assert.assertEquals(SnapshotsInProgress.State.SUCCESS, snapshotStatus.state)
                }
            }

            // Restore follower index on leader cluster
            val restoreSnapshotRequest = RestoreSnapshotRequest(TestCluster.FS_SNAPSHOT_REPO, "test-$snapshotSuffix")
            restoreSnapshotRequest.indices(followerIndexName)
            restoreSnapshotRequest.waitForCompletion(true)
            restoreSnapshotRequest.renamePattern("(.+)")
            restoreSnapshotRequest.renameReplacement("restored-\$1")
            leaderClient.snapshot().restore(restoreSnapshotRequest, RequestOptions.DEFAULT)

            assertBusy {
                assertThat(leaderClient.indices().exists(GetIndexRequest("restored-$followerIndexName"), RequestOptions.DEFAULT)).isEqualTo(true)
            }

            // Invoke stop on the new leader cluster index
            assertThatThrownBy { leaderClient.stopReplication("restored-$followerIndexName") }
                    .isInstanceOf(ResponseException::class.java)
                    .hasMessageContaining("Metadata for restored-$followerIndexName doesn't exist")

            // Start replication on the new leader index
            followerClient.startReplication(
                    StartReplicationRequest("source", "restored-$followerIndexName", "restored-$followerIndexName"),
                    TimeValue.timeValueSeconds(10),
                    true, true
            )

            assertBusy({
                var statusResp = followerClient.replicationStatus("restored-$followerIndexName")
                `validate status syncing response`(statusResp)
                assertThat(followerClient.getShardReplicationTasks("restored-$followerIndexName")).isNotEmpty()
            }, 60, TimeUnit.SECONDS)

        } finally {
            followerClient.stopReplication("restored-$followerIndexName")
            followerClient.stopReplication(followerIndexName)
        }

    }
}
