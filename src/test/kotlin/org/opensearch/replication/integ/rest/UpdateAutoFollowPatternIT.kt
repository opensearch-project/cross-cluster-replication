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

import org.opensearch.replication.MultiClusterAnnotations
import org.opensearch.replication.MultiClusterRestTestCase
import org.opensearch.replication.StartReplicationRequest
import org.opensearch.replication.deleteAutoFollowPattern
import org.opensearch.replication.startReplication
import org.opensearch.replication.stopReplication
import org.opensearch.replication.updateAutoFollowPattern
import org.opensearch.replication.task.autofollow.AutoFollowExecutor
import org.opensearch.replication.task.index.IndexReplicationExecutor
import org.apache.http.HttpStatus
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.apache.logging.log4j.LogManager
import org.assertj.core.api.Assertions
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.ResponseException
import org.opensearch.client.RestHighLevelClient
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.client.indices.GetIndexRequest
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.tasks.TaskInfo
import org.junit.Assert
import java.util.Locale
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.MetadataCreateIndexService
import org.opensearch.replication.AutoFollowStats
import org.opensearch.replication.ReplicationPlugin
import org.opensearch.replication.action.changes.TransportGetChangesAction
import org.opensearch.replication.updateReplicationStartBlockSetting
import org.opensearch.replication.updateAutofollowRetrySetting
import org.opensearch.replication.updateAutoFollowConcurrentStartReplicationJobSetting
import org.opensearch.replication.waitForShardTaskStart
import org.opensearch.test.OpenSearchTestCase.assertBusy
import java.lang.Thread.sleep
import java.util.HashMap
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
    private val longIndexPatternName = "index_".repeat(43)
    private val waitForShardTask = TimeValue.timeValueSeconds(10)

    companion object {
        private val log = LogManager.getLogger(UpdateAutoFollowPatternIT::class.java)
    }

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
            assertBusy ({
                Assertions.assertThat(getAutoFollowTasks(FOLLOWER).size).isEqualTo(1)
            }, 10, TimeUnit.SECONDS)
            leaderIndexNameNew = createRandomIndex(leaderClient)
            // Verify that newly created index on leader which match the pattern are also replicated.
            assertBusy ({
                Assertions.assertThat(followerClient.indices()
                        .exists(GetIndexRequest(leaderIndexNameNew), RequestOptions.DEFAULT))
                        .isEqualTo(true)
                followerClient.waitForShardTaskStart(leaderIndexNameNew, waitForShardTask)
                followerClient.waitForShardTaskStart(leaderIndexName, waitForShardTask)

                var stats = followerClient.AutoFollowStats()
                var af_stats = stats.get("autofollow_stats")!! as ArrayList<HashMap<String, Any>>
                for (key in af_stats) {
                    assert(key["num_success_start_replication"]!! as Int == 2)
                }
                assertTrue(stats.size == 5)
                assertTrue(stats.get("num_success_start_replication")!! == 2)
            }, 60, TimeUnit.SECONDS)
        } finally {
            followerClient.deleteAutoFollowPattern(connectionAlias, indexPatternName)
        }
    }

    fun `test auto follow pattern with updated delay for poll`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        var leaderIndexNameNew = ""
        createConnectionBetweenClusters(FOLLOWER, LEADER, connectionAlias)
        try {
            // Set poll duration to 30sec from 60sec (default)
            val settings = Settings.builder().put(ReplicationPlugin.REPLICATION_AUTOFOLLOW_REMOTE_INDICES_POLL_INTERVAL.key,
                TimeValue.timeValueSeconds(30))
            val clusterUpdateSetttingsReq = ClusterUpdateSettingsRequest().persistentSettings(settings)
            val clusterUpdateResponse = followerClient.cluster().putSettings(clusterUpdateSetttingsReq, RequestOptions.DEFAULT)
            var lastExecutionTime = 0L
            var stats = followerClient.AutoFollowStats()
            Assert.assertTrue(clusterUpdateResponse.isAcknowledged)
            followerClient.updateAutoFollowPattern(connectionAlias, indexPatternName, indexPattern)
            leaderIndexNameNew = createRandomIndex(leaderClient)
            // Verify that newly created index on leader which match the pattern are also replicated.
            assertBusy({
                Assertions.assertThat(followerClient.indices()
                        .exists(GetIndexRequest(leaderIndexNameNew), RequestOptions.DEFAULT))
                        .isEqualTo(true)
                followerClient.waitForShardTaskStart(leaderIndexNameNew, waitForShardTask)
                var af_stats = stats.get("autofollow_stats")!! as ArrayList<HashMap<String, Any>>
                for (key in af_stats) {
                    if(key["name"] == indexPatternName) {
                        Assertions.assertThat(key["last_execution_time"]!! as Long).isNotEqualTo(0L)
                        lastExecutionTime = key["last_execution_time"]!! as Long
                    }
                }
            }, 30, TimeUnit.SECONDS)
            assertBusy({
            var af_stats = stats.get("autofollow_stats")!! as ArrayList<HashMap<String, Any>>
            for (key in af_stats) {
                if(key["name"] == indexPatternName) {
                    Assertions.assertThat(key["last_execution_time"]!! as Long).isNotEqualTo(lastExecutionTime)
                }
            }
            }, 40, TimeUnit.SECONDS)
        } finally {
            followerClient.deleteAutoFollowPattern(connectionAlias, indexPatternName)
        }
    }

    fun `test auto follow pattern with settings`() {
        setMetadataSyncDelay()
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val leaderIndexName = createRandomIndex(leaderClient)
        createConnectionBetweenClusters(FOLLOWER, LEADER, connectionAlias)
        try {
            val settings = Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 3)
                    .build()
            followerClient.updateAutoFollowPattern(connectionAlias, indexPatternName, indexPattern, settings)
            // Verify that existing index matching the pattern are replicated.
            assertBusy ({
                Assertions.assertThat(followerClient.indices()
                        .exists(GetIndexRequest(leaderIndexName), RequestOptions.DEFAULT))
                        .isEqualTo(true)
            }, 30, TimeUnit.SECONDS)
            Assertions.assertThat(getAutoFollowTasks(FOLLOWER).size).isEqualTo(1)
            val getSettingsRequest = GetSettingsRequest()
            getSettingsRequest.indices(leaderIndexName)
            getSettingsRequest.includeDefaults(true)
            assertBusy ({
                Assert.assertEquals(
                        "3",
                        followerClient.indices()
                                .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                                .indexToSettings[leaderIndexName][IndexMetadata.SETTING_NUMBER_OF_REPLICAS]
                )
                followerClient.waitForShardTaskStart(leaderIndexName, waitForShardTask)
            }, 15, TimeUnit.SECONDS)
        } finally {
            followerClient.deleteAutoFollowPattern(connectionAlias, indexPatternName)
        }
    }
    
    fun `test auto follow stats`() {
        val indexPatternName2 = "test_pattern2"
        val indexPattern2 = "lead_index*"
        val leaderIndexName2 = "lead_index1"
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val leaderIndexName = createRandomIndex(leaderClient)
        leaderClient.indices().create(CreateIndexRequest(leaderIndexName2), RequestOptions.DEFAULT)
        createConnectionBetweenClusters(FOLLOWER, LEADER, connectionAlias)
        try {
            followerClient.updateAutoFollowPattern(connectionAlias, indexPatternName, indexPattern)
            followerClient.updateAutoFollowPattern(connectionAlias, indexPatternName2, indexPattern2)
            assertBusy ({
                Assertions.assertThat(getAutoFollowTasks(FOLLOWER).size).isEqualTo(2)
            }, 30, TimeUnit.SECONDS)
            // Verify that existing index matching the pattern are replicated.
            assertBusy ({
                Assertions.assertThat(followerClient.indices()
                        .exists(GetIndexRequest(leaderIndexName2), RequestOptions.DEFAULT))
                        .isEqualTo(true)
                var stats = followerClient.AutoFollowStats()
                Assertions.assertThat(stats.size).isEqualTo(5)
                assert(stats["num_success_start_replication"]!! as Int == 2)
                var af_stats = stats.get("autofollow_stats")!! as ArrayList<HashMap<String, Any>>
                for (key in af_stats) {
                    assert(key["num_success_start_replication"]!! as Int == 1)
                }
                assertTrue(af_stats.size == 2)
            }, 30, TimeUnit.SECONDS)
        } finally {
            followerClient.deleteAutoFollowPattern(connectionAlias, indexPatternName)
            followerClient.deleteAutoFollowPattern(connectionAlias, indexPatternName2)
            followerClient.waitForShardTaskStart(leaderIndexName)
            followerClient.waitForShardTaskStart(leaderIndexName2)
        }
    }

    fun `test auto follow shouldn't add already triggered index`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val leaderIndexName = createRandomIndex(leaderClient)
        createConnectionBetweenClusters(FOLLOWER, LEADER, connectionAlias)
        followerClient.startReplication(StartReplicationRequest(connectionAlias, leaderIndexName, leaderIndexName),
                TimeValue.timeValueSeconds(10),true, waitForRestore = true)
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
            assertBusy({
                // Assert that there is still only one index replication task
                Assertions.assertThat(getAutoFollowTasks(FOLLOWER).size).isEqualTo(1)
                Assertions.assertThat(getIndexReplicationTasks(FOLLOWER).size).isEqualTo(1)
                followerClient.waitForShardTaskStart(leaderIndexName, waitForShardTask)
            },30, TimeUnit.SECONDS)
        } finally {
            followerClient.deleteAutoFollowPattern(connectionAlias, indexPatternName)
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

    fun `test auto follow should fail on pattern name validation failure`() {
        val followerClient = getClientForCluster(FOLLOWER)
        createConnectionBetweenClusters(FOLLOWER, LEADER, connectionAlias)
        assertPatternNameValidation(followerClient, "testPattern",
            "Value testPattern must be lowercase")
        assertPatternNameValidation(followerClient, "testPattern*",
            "Value testPattern* must not contain the following characters")
        assertPatternNameValidation(followerClient, "test#",
            "Value test# must not contain '#' or ':'")
        assertPatternNameValidation(followerClient, "test:",
            "Value test: must not contain '#' or ':'")
        assertPatternNameValidation(followerClient, ".",
            "Value . must not be '.' or '..'")
        assertPatternNameValidation(followerClient, "..",
            "Value .. must not be '.' or '..'")
        assertPatternNameValidation(followerClient, "_leader",
            "Value _leader must not start with '_' or '-' or '+'")
        assertPatternNameValidation(followerClient, "-leader",
            "Value -leader must not start with '_' or '-' or '+'")
        assertPatternNameValidation(followerClient, "+leader",
            "Value +leader must not start with '_' or '-' or '+'")
        assertPatternNameValidation(followerClient, longIndexPatternName,
            "Value $longIndexPatternName must not be longer than ${MetadataCreateIndexService.MAX_INDEX_NAME_BYTES} bytes")
        assertPatternNameValidation(followerClient, ".leaderIndex",
            "Value .leaderIndex must not start with '.'")
    }

    private fun assertPatternNameValidation(followerClient: RestHighLevelClient, patternName: String,
        errorMsg: String) {
        Assertions.assertThatThrownBy {
            followerClient.updateAutoFollowPattern(connectionAlias, patternName, indexPattern)
        }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining(errorMsg)
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
            followerClient.waitForShardTaskStart(leaderIndexName, waitForShardTask)
        } finally {
            followerClient.deleteAutoFollowPattern(connectionAlias, indexPatternName)
        }
        // Verify that auto follow tasks is stopped but the shard replication task remains.
        assertBusy ({
            Assertions.assertThat(getAutoFollowTasks(FOLLOWER).size).isEqualTo(0)
        }, 30, TimeUnit.SECONDS)
        Assertions.assertThat(getIndexReplicationTasks(FOLLOWER).size).isEqualTo(1)
    }

    fun `test autofollow task with start replication block and retries`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER, connectionAlias)
        try {
            //modify retry duration to account for autofollow trigger in next retry
            followerClient.updateAutofollowRetrySetting("1m")
            for (repeat in 1..2) {
                log.info("Current Iteration $repeat")
                // Add replication start block
                followerClient.updateReplicationStartBlockSetting(true)
                createRandomIndex(leaderClient)
                followerClient.updateAutoFollowPattern(connectionAlias, indexPatternName, indexPattern)
                sleep(95000) // wait for auto follow trigger in the worst case
                // verify both index replication tasks and autofollow tasks
                // Replication shouldn't have been started - (repeat-1) tasks as for current loop index shouldn't be
                // created yet.
                // Autofollow task should still be up - 1 task
                Assertions.assertThat(getIndexReplicationTasks(FOLLOWER).size).isEqualTo(repeat-1)
                Assertions.assertThat(getAutoFollowTasks(FOLLOWER).size).isEqualTo(1)

                var stats = followerClient.AutoFollowStats()
                var failedIndices = stats["failed_indices"] as List<*>
                // Every time failed replication task will be 1 as
                // there are already running jobs in the previous iteration
                log.info("Current failed indices $failedIndices")
                assert(failedIndices.size == 1)
                // Remove replication start block
                followerClient.updateReplicationStartBlockSetting(false)
                sleep(95000) // wait for auto follow trigger in the worst case
                // Index should be replicated and autofollow task should be present
                Assertions.assertThat(getIndexReplicationTasks(FOLLOWER).size).isEqualTo(repeat)
                Assertions.assertThat(getAutoFollowTasks(FOLLOWER).size).isEqualTo(1)
                stats = followerClient.AutoFollowStats()
                failedIndices = stats["failed_indices"] as List<*>
                assert(failedIndices.isEmpty())
            }
        } finally {
            followerClient.deleteAutoFollowPattern(connectionAlias, indexPatternName)
        }
    }

    fun `test autofollow task with concurrent job setting set to run parallel jobs`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER, connectionAlias)
        // create two leader indices and test autofollow to trigger to trigger jobs based on setting
        val leaderIndexName1 = createRandomIndex(leaderClient)
        val leaderIndexName2 = createRandomIndex(leaderClient)
        followerClient.updateAutoFollowConcurrentStartReplicationJobSetting(2)
        try {
            followerClient.updateAutoFollowPattern(connectionAlias, indexPatternName, indexPattern)
            // Verify that existing index matching the pattern are replicated.
            assertBusy {
                Assertions.assertThat(followerClient.indices()
                    .exists(GetIndexRequest(leaderIndexName1), RequestOptions.DEFAULT))
                    .isEqualTo(true)
            }
            assertBusy {
                Assertions.assertThat(followerClient.indices()
                    .exists(GetIndexRequest(leaderIndexName2), RequestOptions.DEFAULT))
                    .isEqualTo(true)
            }
            sleep(30000) // Default poll for auto follow in worst case
            Assertions.assertThat(getAutoFollowTasks(FOLLOWER).size).isEqualTo(1)
        } finally {
            // Reset default autofollow setting
            followerClient.updateAutoFollowConcurrentStartReplicationJobSetting(null)
            followerClient.deleteAutoFollowPattern(connectionAlias, indexPatternName)
        }
    }

    fun `test autofollow task with concurrent job setting set to run single job`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER, connectionAlias)
        // create two leader indices and test autofollow to trigger to trigger jobs based on setting
        val leaderIndexName1 = createRandomIndex(leaderClient)
        val leaderIndexName2 = createRandomIndex(leaderClient)
        followerClient.updateAutoFollowConcurrentStartReplicationJobSetting(1)
        try {
            followerClient.updateAutoFollowPattern(connectionAlias, indexPatternName, indexPattern)
            // Verify that existing index matching the pattern are replicated.
            assertBusy {
                // check that the index replication task is created for only index
                Assertions.assertThat(getIndexReplicationTasks(FOLLOWER).size).isEqualTo(1)
            }
            sleep(30000) // Default poll for auto follow in worst case
            assertBusy {
                // check that the index replication task is created for only index
                Assertions.assertThat(getIndexReplicationTasks(FOLLOWER).size).isEqualTo(2)
            }
            sleep(30000) // Default poll for auto follow in worst case
            Assertions.assertThat(getAutoFollowTasks(FOLLOWER).size).isEqualTo(1)
        } finally {
            // Reset default autofollow setting
            followerClient.updateAutoFollowConcurrentStartReplicationJobSetting(null)
            followerClient.deleteAutoFollowPattern(connectionAlias, indexPatternName)
        }
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