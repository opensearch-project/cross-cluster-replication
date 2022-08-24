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
import com.amazon.elasticsearch.replication.ReplicationPlugin.Companion.PLUGINS_REPLICATION_TRANSLOG_PRUNING_SETTING
import com.amazon.elasticsearch.replication.StartReplicationRequest
import com.amazon.elasticsearch.replication.`validate not paused status response`
import com.amazon.elasticsearch.replication.`validate paused status on closed index`
import com.amazon.elasticsearch.replication.pauseReplication
import com.amazon.elasticsearch.replication.replicationStatus
import com.amazon.elasticsearch.replication.resumeReplication
import com.amazon.elasticsearch.replication.`validate paused status response due to leader index deleted`
import com.amazon.elasticsearch.replication.`validate status syncing response`
import com.amazon.elasticsearch.replication.followerStats
import com.amazon.elasticsearch.replication.leaderStats
import com.amazon.elasticsearch.replication.startReplication
import com.amazon.elasticsearch.replication.stopReplication
import com.amazon.elasticsearch.replication.updateAutoFollowPattern
import com.amazon.elasticsearch.replication.updateReplication
import com.amazon.elasticsearch.replication.updateReplicationStartBlockSetting
import org.apache.http.HttpStatus
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.apache.http.util.EntityUtils
import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest
import org.elasticsearch.action.admin.indices.alias.Alias
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Request
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.ResponseException
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.core.CountRequest
import org.elasticsearch.client.indices.CloseIndexRequest
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.client.indices.GetMappingsRequest
import org.elasticsearch.client.indices.PutMappingRequest
import org.elasticsearch.cluster.metadata.AliasMetadata
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService
import org.elasticsearch.common.io.PathUtils
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.IndexSettings
import org.elasticsearch.index.mapper.MapperService
import org.elasticsearch.repositories.fs.FsRepository
import org.elasticsearch.test.ESTestCase.assertBusy
import org.junit.Assert
import java.lang.Thread.sleep
import java.nio.file.Files
import java.util.concurrent.TimeUnit


@MultiClusterAnnotations.ClusterConfigurations(
        MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
        MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)
class StartReplicationIT: MultiClusterRestTestCase() {
    private val leaderIndexName = "leader_index"
    private val followerIndexName = "follower_index"
    private val leaderClusterPath = "testclusters/leaderCluster-0"
    private val followerClusterPath = "testclusters/followCluster-0"
    private val repoPath = "testclusters/repo"
    private val buildDir = System.getProperty("build.dir")
    private val synonymsJson = "/analyzers/synonym_setting.json"
    private val synonymMapping = "{\"properties\":{\"value\":{\"type\":\"text\",\"analyzer\":\"standard\",\"search_analyzer\":\"my_analyzer\"}}}"
    private val longIndexName = "index_".repeat(43)
    // 3x of SLEEP_TIME_BETWEEN_POLL_MS
    val SLEEP_TIME_BETWEEN_SYNC = 15L

    fun `test start replication in following state and empty index`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))
            assertBusy {
                assertThat(followerClient.indices().exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT)).isEqualTo(true)
            }
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test start replication with settings`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        val settings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 3)
                .build()
        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName, settings = settings))
            assertBusy {
                assertThat(followerClient.indices().exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT)).isEqualTo(true)
            }

            val getSettingsRequest = GetSettingsRequest()
            getSettingsRequest.indices(followerIndexName)
            getSettingsRequest.includeDefaults(true)
            assertBusy ({
                Assert.assertEquals(
                        "3",
                        followerClient.indices()
                                .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                                .indexToSettings[followerIndexName][IndexMetadata.SETTING_NUMBER_OF_REPLICAS]
                )
            }, 15, TimeUnit.SECONDS)
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }


    fun `test replication when _close is triggered on leader`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName), waitForRestore = true)
            assertThat(followerClient.indices().exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT)).isEqualTo(true)
            leaderClient.lowLevelClient.performRequest(Request("POST", "/" + leaderIndexName + "/_close"))
            assertBusy ({
                try {
                    assertThat(followerClient.replicationStatus(followerIndexName)).containsKey("status")
                    var statusResp = followerClient.replicationStatus(followerIndexName)
                    `validate paused status on closed index`(statusResp)
                } catch (e : Exception) {
                    Assert.fail()
                }
            },30, TimeUnit.SECONDS)
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }


    fun `test replication when _close and _open is triggered on leader`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName), waitForRestore = true)
            followerClient.pauseReplication(followerIndexName)
            leaderClient.lowLevelClient.performRequest(Request("POST", "/" + leaderIndexName + "/_close"))
            leaderClient.lowLevelClient.performRequest(Request("POST", "/" + leaderIndexName + "/_open"))
            followerClient.resumeReplication(followerIndexName)
            var statusResp = followerClient.replicationStatus(followerIndexName)
            `validate not paused status response`(statusResp)

        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test start replication fails when replication has already been started for the same index`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))
        assertThatThrownBy {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))
        }.isInstanceOf(ResponseException::class.java).hasMessageContaining("Cant use same index again for replication." +
                " Delete the index:$followerIndexName")
    }

    fun `test start replication fails when remote cluster alias does not exist`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        assertThatThrownBy {
            followerClient.startReplication(StartReplicationRequest("does_not_exist", leaderIndexName, followerIndexName))
        }.isInstanceOf(ResponseException::class.java).hasMessageContaining("{\"error\":{\"root_cause\":[{\"type\":\"no_such_remote_cluster_exception\"," +
                "\"reason\":\"no such remote cluster: [does_not_exist]\"}]")
    }

    fun `test start replication fails when index does not exist`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        assertThatThrownBy {
            followerClient.startReplication(StartReplicationRequest("source", "does_not_exist", followerIndexName))
        }.isInstanceOf(ResponseException::class.java).hasMessageContaining("{\"error\":{\"root_cause\":[{\"type\":\"index_not_found_exception\"," +
                "\"reason\":\"no such index [does_not_exist]\"")
    }

    fun `test start replication fails when the follower cluster is write blocked or metadata blocked`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        addClusterMetadataBlock(FOLLOWER, "true")
        assertThatThrownBy {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))
        }.isInstanceOf(ResponseException::class.java).hasMessageContaining("{\"error\":{\"root_cause\":[{\"type\":\"cluster_block_exception\"," +
                "\"reason\":\"blocked by: [FORBIDDEN/6/cluster read-only (api)];\"}]")
        // Removing the metadata block for cleanup
        addClusterMetadataBlock(FOLLOWER, "false")
    }

    fun `test that follower index has same mapping as leader index`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))
            assertBusy {
                assertThat(followerClient.indices()
                        .exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT))
                        .isEqualTo(true)
            }
            Assert.assertEquals(
                    leaderClient.indices().getMapping(GetMappingsRequest().indices(leaderIndexName), RequestOptions.DEFAULT)
                            .mappings()[leaderIndexName],
                    followerClient.indices().getMapping(GetMappingsRequest().indices(followerIndexName), RequestOptions.DEFAULT)
                            .mappings()[followerIndexName]
            )
            // test that new mapping created on leader is also propagated to follower
            val putMappingRequest = PutMappingRequest(leaderIndexName)
            putMappingRequest.source("{\"properties\":{\"name\":{\"type\":\"keyword\"}}}", XContentType.JSON)
            leaderClient.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT)
            val sourceMap = mapOf("name" to randomAlphaOfLength(5))
            leaderClient.index(IndexRequest(leaderIndexName).id("1").source(sourceMap), RequestOptions.DEFAULT)
            val leaderMappings = leaderClient.indices().getMapping(GetMappingsRequest().indices(leaderIndexName), RequestOptions.DEFAULT)
                .mappings()[leaderIndexName]
            assertBusy {
                Assert.assertEquals(
                    leaderMappings,
                    followerClient.indices().getMapping(GetMappingsRequest().indices(followerIndexName), RequestOptions.DEFAULT)
                        .mappings()[followerIndexName]
                )
            }

        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test that index settings are getting replicated`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val settings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName).settings(settings), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))
            assertBusy {
                assertThat(followerClient.indices()
                        .exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT))
                        .isEqualTo(true)
            }
            val getSettingsRequest = GetSettingsRequest()
            getSettingsRequest.indices(followerIndexName)
            getSettingsRequest.names(IndexMetadata.SETTING_NUMBER_OF_REPLICAS)
            Assert.assertEquals(
                    "0",
                    followerClient.indices()
                            .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                            .indexToSettings[followerIndexName][IndexMetadata.SETTING_NUMBER_OF_REPLICAS]
            )
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test that aliases settings are getting replicated`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName)
            .alias(Alias("leaderAlias").filter("{\"term\":{\"year\":2016}}").routing("1"))
            , RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))
            assertBusy {
                assertThat(followerClient.indices()
                        .exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT))
                        .isEqualTo(true)
            }
            assertBusy({
                Assert.assertEquals(
                    leaderClient.indices().getAlias(GetAliasesRequest().indices(leaderIndexName),
                        RequestOptions.DEFAULT).aliases[leaderIndexName],
                    followerClient.indices().getAlias(GetAliasesRequest().indices(followerIndexName),
                        RequestOptions.DEFAULT).aliases[followerIndexName]
                )

            }, 30L, TimeUnit.SECONDS)
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test that replication cannot be started on leader alias directly`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER, "source")

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName).alias(Alias("leader_alias")), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()

        try {
            followerClient.startReplication(StartReplicationRequest("source", "leader_alias", followerIndexName))
            fail("Expected startReplication to fail")
        } catch (e: ResponseException) {
            assertThat(e.response.statusLine.statusCode).isEqualTo(404)
            assertThat(e.message).contains("index_not_found_exception")
            assertThat(e.message).contains("no such index [source:leader_alias]")
        }
    }

    fun `test that translog settings are set on leader and not on follower`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))
            assertBusy {
                assertThat(followerClient.indices()
                        .exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT))
                        .isEqualTo(true)
                assertThat(followerClient.indices()
                        .getSettings(GetSettingsRequest().indices(followerIndexName), RequestOptions.DEFAULT)
                        .getSetting(followerIndexName, PLUGINS_REPLICATION_TRANSLOG_PRUNING_SETTING)
                        .isNullOrEmpty())
            }

            assertThat(leaderClient.indices()
                    .getSettings(GetSettingsRequest().indices(leaderIndexName), RequestOptions.DEFAULT)
                    .getSetting(leaderIndexName, PLUGINS_REPLICATION_TRANSLOG_PRUNING_SETTING) == "true")

        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test that translog settings are set on leader`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))

            val leaderSettings = leaderClient.indices()
                    .getSettings(GetSettingsRequest().indices(leaderIndexName), RequestOptions.DEFAULT)
            assertThat(leaderSettings.getSetting(leaderIndexName,
                            IndexSettings.INDEX_PLUGINS_REPLICATION_TRANSLOG_RETENTION_LEASE_PRUNING_ENABLED_SETTING.key) == "true")
            assertThat(leaderSettings.getSetting(leaderIndexName,
                            IndexSettings.INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING.key) == "32mb")

        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test that replication continues after removing translog settings based on retention lease`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName),
                    waitForRestore = true)
            assertBusy {
                assertThat(followerClient.indices()
                        .exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT))
                        .isEqualTo(true)
            }
            // Turn-off the settings and index doc
            val settingsBuilder = Settings.builder().put(PLUGINS_REPLICATION_TRANSLOG_PRUNING_SETTING, false)
            val settingsUpdateResponse = leaderClient.indices().putSettings(UpdateSettingsRequest(leaderIndexName)
                    .settings(settingsBuilder.build()), RequestOptions.DEFAULT)
            Assert.assertEquals(settingsUpdateResponse.isAcknowledged, true)
            val sourceMap = mapOf("name" to randomAlphaOfLength(5))
            leaderClient.index(IndexRequest(leaderIndexName).id("2").source(sourceMap), RequestOptions.DEFAULT)
            assertBusy {
                followerClient.get(GetRequest(followerIndexName).id("2"), RequestOptions.DEFAULT).isExists
            }
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    private fun addClusterMetadataBlock(clusterName: String, blockValue: String) {
        val cluster = getNamedCluster(clusterName)
        val persistentConnectionRequest = Request("PUT", "_cluster/settings")
        val entityAsString = """
                        {
                          "persistent": {
                             "cluster": {
                               "blocks": {
                                 "read_only": $blockValue
                               }
                             }
                          }
                        }""".trimMargin()

        persistentConnectionRequest.entity = NStringEntity(entityAsString, ContentType.APPLICATION_JSON)
        val persistentConnectionResponse = cluster.lowLevelClient.performRequest(persistentConnectionRequest)
        assertEquals(HttpStatus.SC_OK.toLong(), persistentConnectionResponse.statusLine.statusCode.toLong())
    }

    fun `test that dynamic index settings and alias are getting replicated `() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        setMetadataSyncDelay()

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        var settings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName).settings(settings), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))
            assertBusy {
                assertThat(followerClient.indices()
                        .exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT))
                        .isEqualTo(true)
            }

            settings = Settings.builder()
                    .build()

            followerClient.updateReplication( followerIndexName, settings)

            val getSettingsRequest = GetSettingsRequest()
            getSettingsRequest.indices(followerIndexName)
            getSettingsRequest.includeDefaults(true)
            Assert.assertEquals(
                    "0",
                    followerClient.indices()
                            .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                            .indexToSettings[followerIndexName][IndexMetadata.SETTING_NUMBER_OF_REPLICAS]
            )

            settings = Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
                    .put("routing.allocation.enable", "none")
                    .build()

            leaderClient.indices().putSettings(UpdateSettingsRequest(leaderIndexName).settings(settings), RequestOptions.DEFAULT)

            var indicesAliasesRequest = IndicesAliasesRequest()
            var aliasAction = IndicesAliasesRequest.AliasActions.add()
                    .index(leaderIndexName)
                    .alias("alias1").filter("{\"term\":{\"year\":2016}}").routing("1")
            indicesAliasesRequest.addAliasAction(aliasAction)
            leaderClient.indices().updateAliases(indicesAliasesRequest, RequestOptions.DEFAULT)

            TimeUnit.SECONDS.sleep(SLEEP_TIME_BETWEEN_SYNC)
            getSettingsRequest.indices(followerIndexName)
            // Leader setting is copied
            Assert.assertEquals(
                    "2",
                    followerClient.indices()
                            .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                            .indexToSettings[followerIndexName][IndexMetadata.SETTING_NUMBER_OF_REPLICAS]
            )
            assertEqualAliases()

            // Case 2 :  Blocklisted  setting are not copied
            Assert.assertNull(followerClient.indices()
                    .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                    .indexToSettings[followerIndexName].get("index.routing.allocation.enable"))

            //Alias test case 2: Update existing alias
            aliasAction = IndicesAliasesRequest.AliasActions.add()
                    .index(leaderIndexName)
                    .routing("2")
                    .alias("alias1")
                    .writeIndex(true)
                    .isHidden(false)
            indicesAliasesRequest.addAliasAction(aliasAction)
            leaderClient.indices().updateAliases(indicesAliasesRequest, RequestOptions.DEFAULT)

            //Use Update API
            settings = Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 3)
                    .put("index.routing.allocation.enable", "none")
                    .put("index.search.idle.after", "10s")
                    .build()

            followerClient.updateReplication( followerIndexName, settings)
            TimeUnit.SECONDS.sleep(SLEEP_TIME_BETWEEN_SYNC)

            // Case 3 : Updated Settings take higher priority.  Blocklisted  settins shouldn't matter for that
            Assert.assertEquals(
                    "3",
                    followerClient.indices()
                            .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                            .indexToSettings[followerIndexName][IndexMetadata.SETTING_NUMBER_OF_REPLICAS]
            )

            Assert.assertEquals(
                    "10s",
                    followerClient.indices()
                            .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                            .indexToSettings[followerIndexName]["index.search.idle.after"]
            )

            Assert.assertEquals(
                    "none",
                    followerClient.indices()
                            .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                            .indexToSettings[followerIndexName]["index.routing.allocation.enable"]
            )

            assertEqualAliases()

            //Clear the settings
            settings = Settings.builder()
                    .build()
            followerClient.updateReplication( followerIndexName, settings)

            //Alias test case 3: Delete one alias and add another alias
            aliasAction = IndicesAliasesRequest.AliasActions.remove()
                    .index(leaderIndexName)
                    .alias("alias1")
            indicesAliasesRequest.addAliasAction(aliasAction
            )
            leaderClient.indices().updateAliases(indicesAliasesRequest, RequestOptions.DEFAULT)
            var aliasAction2 = IndicesAliasesRequest.AliasActions.add()
                    .index(leaderIndexName)
                    .routing("12")
                    .alias("alias2")
                    .indexRouting("indexRouting")
            indicesAliasesRequest.addAliasAction(aliasAction2)

            TimeUnit.SECONDS.sleep(SLEEP_TIME_BETWEEN_SYNC)

            Assert.assertEquals(
                    null,
                    followerClient.indices()
                            .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                            .indexToSettings[followerIndexName]["index.search.idle.after"]
            )

            assertEqualAliases()
        } finally {
            followerClient.stopReplication(followerIndexName)
        }

    }

    fun `test that static index settings are getting replicated `() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        setMetadataSyncDelay()

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        var settings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .build()

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName).settings(settings), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))
            assertBusy {
                assertThat(followerClient.indices()
                        .exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT))
                        .isEqualTo(true)
            }
            val getSettingsRequest = GetSettingsRequest()
            getSettingsRequest.indices(followerIndexName)
            Assert.assertEquals(
                    "1",
                    followerClient.indices()
                            .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                            .indexToSettings[followerIndexName][IndexMetadata.SETTING_NUMBER_OF_REPLICAS]
            )

            settings = Settings.builder()
                    .put("index.shard.check_on_startup", "checksum")
                    .build()
            followerClient.updateReplication(followerIndexName, settings)

            TimeUnit.SECONDS.sleep(SLEEP_TIME_BETWEEN_SYNC)
            Assert.assertEquals(
                    "checksum",
                    followerClient.indices()
                            .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                            .indexToSettings[followerIndexName]["index.shard.check_on_startup"]
            )
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test that replication fails to start when custom analyser is not present in follower`() {
        val synonyms = javaClass.getResourceAsStream("/analyzers/synonyms.txt")
        val config = PathUtils.get(buildDir, leaderClusterPath, "config")
        val synonymPath = config.resolve("synonyms.txt")
        try {
            Files.copy(synonyms, synonymPath)

            val settings: Settings = Settings.builder().loadFromStream(synonymsJson, javaClass.getResourceAsStream(synonymsJson), false)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .build()

            val leaderClient = getClientForCluster(LEADER)
            val followerClient = getClientForCluster(FOLLOWER)
            val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName)
                .settings(settings).mapping(synonymMapping, XContentType.JSON), RequestOptions.DEFAULT)
            assertThat(createIndexResponse.isAcknowledged).isTrue()

            createConnectionBetweenClusters(FOLLOWER, LEADER)

            assertThatThrownBy {
                followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))
            }.isInstanceOf(ResponseException::class.java).hasMessageContaining("resource_not_found_exception")
        } finally {
            if (Files.exists(synonymPath)) {
                Files.delete(synonymPath)
            }
        }
    }

    fun `test that replication starts successfully when custom analyser is present in follower`() {
        val synonyms = javaClass.getResourceAsStream("/analyzers/synonyms.txt")
        val leaderConfig = PathUtils.get(buildDir, leaderClusterPath, "config")
        val leaderSynonymPath = leaderConfig.resolve("synonyms.txt")
        val followerConfig = PathUtils.get(buildDir, followerClusterPath, "config")
        val followerSynonymPath = followerConfig.resolve("synonyms.txt")
        try {
            Files.copy(synonyms, leaderSynonymPath)
            Files.copy(synonyms, followerSynonymPath)

            val settings: Settings = Settings.builder().loadFromStream(synonymsJson, javaClass.getResourceAsStream(synonymsJson), false)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .build()

            val leaderClient = getClientForCluster(LEADER)
            val followerClient = getClientForCluster(FOLLOWER)
            val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName)
                .settings(settings).mapping(synonymMapping, XContentType.JSON), RequestOptions.DEFAULT)
            assertThat(createIndexResponse.isAcknowledged).isTrue()

            createConnectionBetweenClusters(FOLLOWER, LEADER)

            try {
                followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))
                assertBusy {
                    assertThat(followerClient.indices().exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT)).isEqualTo(true)
                }
            } finally {
                followerClient.stopReplication(followerIndexName)
            }
        } finally {
            if (Files.exists(leaderSynonymPath)) {
                Files.delete(leaderSynonymPath)
            }
            if (Files.exists(followerSynonymPath)) {
                Files.delete(followerSynonymPath)
            }
        }
    }

    fun `test that replication starts successfully when custom analyser is overridden and present in follower`() {
        val synonyms = javaClass.getResourceAsStream("/analyzers/synonyms.txt")
        val leaderConfig = PathUtils.get(buildDir, leaderClusterPath, "config")
        val leaderSynonymPath = leaderConfig.resolve("synonyms.txt")
        val followerConfig = PathUtils.get(buildDir, followerClusterPath, "config")
        val synonymFollowerFilename = "synonyms_follower.txt"
        val followerSynonymPath = followerConfig.resolve(synonymFollowerFilename)
        try {
            Files.copy(synonyms, leaderSynonymPath)
            Files.copy(synonyms, followerSynonymPath)

            val settings: Settings = Settings.builder().loadFromStream(synonymsJson, javaClass.getResourceAsStream(synonymsJson), false)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .build()

            val leaderClient = getClientForCluster(LEADER)
            val followerClient = getClientForCluster(FOLLOWER)
            val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName)
                .settings(settings).mapping(synonymMapping, XContentType.JSON), RequestOptions.DEFAULT)
            assertThat(createIndexResponse.isAcknowledged).isTrue()

            createConnectionBetweenClusters(FOLLOWER, LEADER)

            try {
                val overriddenSettings: Settings = Settings.builder()
                    .put("index.analysis.filter.my_filter.synonyms_path", synonymFollowerFilename)
                    .build()
                followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName, overriddenSettings))
                assertBusy {
                    assertThat(followerClient.indices().exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT)).isEqualTo(true)
                }
            } finally {
                followerClient.stopReplication(followerIndexName)
            }
        } finally {
            if (Files.exists(leaderSynonymPath)) {
                Files.delete(leaderSynonymPath)
            }
            if (Files.exists(followerSynonymPath)) {
                Files.delete(followerSynonymPath)
            }
        }
    }

    fun `test that follower index cannot be deleted after starting replication`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()

        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))
            assertBusy {
                assertThat(followerClient.indices().exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT)).isEqualTo(true)
            }
            // Need to wait till index blocks appear into state
            assertBusy {
                val clusterBlocksResponse = followerClient.lowLevelClient.performRequest(Request("GET", "/_cluster/state/blocks"))
                val clusterResponseString = EntityUtils.toString(clusterBlocksResponse.entity)
                assertThat(clusterResponseString.contains("cross-cluster-replication"))
                    .withFailMessage("Cant find replication block afer starting replication")
                    .isTrue()
            }
            // Delete index
            assertThatThrownBy {
                followerClient.indices().delete(DeleteIndexRequest(followerIndexName), RequestOptions.DEFAULT)
            }.isInstanceOf(ElasticsearchStatusException::class.java).hasMessageContaining("cluster_block_exception")
            // Close index
            assertThatThrownBy {
                followerClient.indices().close(CloseIndexRequest(followerIndexName), RequestOptions.DEFAULT)
            }.isInstanceOf(ElasticsearchStatusException::class.java).hasMessageContaining("cluster_block_exception")
            // Index document
            assertThatThrownBy {
                val sourceMap = mapOf("name" to randomAlphaOfLength(5))
                followerClient.index(IndexRequest(followerIndexName).id("1").source(sourceMap), RequestOptions.DEFAULT)
            }.isInstanceOf(ElasticsearchStatusException::class.java).hasMessageContaining("cluster_block_exception")
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test that replication gets paused if the leader index is deleted`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()

        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))
            assertBusy {
                assertThat(followerClient.indices().exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT)).isEqualTo(true)
            }
            assertBusy {
                var statusResp = followerClient.replicationStatus(followerIndexName)
                `validate status syncing response`(statusResp)
            }
            val deleteIndexResponse = leaderClient.indices().delete(DeleteIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
            assertThat(deleteIndexResponse.isAcknowledged).isTrue()

            assertBusy({
                var statusResp = followerClient.replicationStatus(followerIndexName)
                `validate paused status response due to leader index deleted`(statusResp)
            }, 15, TimeUnit.SECONDS)
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test forcemerge on leader during replication bootstrap`() {
        val settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 20)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.key, Long.MAX_VALUE)
            .build()
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName).settings(settings),
            RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        // Put a large amount of data into the index
        IndexUtil.fillIndex(leaderClient, leaderIndexName, 5000, 1000, 1000)
        assertBusy {
            assertThat(leaderClient.indices()
                .exists(GetIndexRequest(leaderIndexName), RequestOptions.DEFAULT))
        }
        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName),
                TimeValue.timeValueSeconds(10),
                false)
            //Given the size of index, the replication should be in RESTORING phase at this point
            leaderClient.indices().forcemerge(ForceMergeRequest(leaderIndexName), RequestOptions.DEFAULT)

            assertBusy {
                var statusResp = followerClient.replicationStatus(followerIndexName)
                `validate status syncing response`(statusResp)
            }
            TimeUnit.SECONDS.sleep(30)
            assertBusy ({
                Assert.assertEquals(leaderClient.count(CountRequest(leaderIndexName), RequestOptions.DEFAULT).toString(),
                    followerClient.count(CountRequest(followerIndexName), RequestOptions.DEFAULT).toString())
                },
                30, TimeUnit.SECONDS
            )
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test that snapshot on leader does not affect replication during bootstrap`() {
        val settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 20)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.key, Long.MAX_VALUE)
            .build()
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val repoPath = PathUtils.get(buildDir, repoPath)

        val putRepositoryRequest = PutRepositoryRequest("my-repo")
            .type(FsRepository.TYPE)
            .settings("{\"location\": \"$repoPath\"}", XContentType.JSON)

        leaderClient.snapshot().createRepository(putRepositoryRequest, RequestOptions.DEFAULT)

        val createIndexResponse = leaderClient.indices().create(
            CreateIndexRequest(leaderIndexName).settings(settings),
            RequestOptions.DEFAULT
        )
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        // Put a large amount of data into the index
        IndexUtil.fillIndex(leaderClient, leaderIndexName, 5000, 1000, 1000)
        assertBusy {
            assertThat(
                leaderClient.indices()
                    .exists(GetIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
            )
        }
        try {
            followerClient.startReplication(
                StartReplicationRequest("source", leaderIndexName, followerIndexName),
                TimeValue.timeValueSeconds(10),
                false
            )
            //Given the size of index, the replication should be in RESTORING phase at this point
            leaderClient.snapshot().create(CreateSnapshotRequest("my-repo", "snapshot_1").indices(leaderIndexName), RequestOptions.DEFAULT)

            assertBusy({
                var statusResp = followerClient.replicationStatus(followerIndexName)
                `validate status syncing response`(statusResp)
            }, 30, TimeUnit.SECONDS
            )
            assertBusy({
                Assert.assertEquals(
                    leaderClient.count(CountRequest(leaderIndexName), RequestOptions.DEFAULT).toString(),
                    followerClient.count(CountRequest(followerIndexName), RequestOptions.DEFAULT).toString()
                )},
                30, TimeUnit.SECONDS
            )
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test that replication cannot be started when soft delete is disabled`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val settings: Settings = Settings.builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.key, false)
            .build()

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName)
            .settings(settings), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()

        assertThatThrownBy {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))
        }.isInstanceOf(ResponseException::class.java).hasMessageContaining("Cannot Replicate an index where the setting index.soft_deletes.enabled is disabled")
    }

    fun `test leader stats`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val settings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                .build()

        val createIndexResponse = leaderClient.indices().create(
                CreateIndexRequest(leaderIndexName).settings(settings),
                RequestOptions.DEFAULT
        )
        assertThat(createIndexResponse.isAcknowledged).isTrue()

        try {
            followerClient.startReplication(
                    StartReplicationRequest("source", leaderIndexName, followerIndexName),
                    TimeValue.timeValueSeconds(10),
                    true
            )

            val docCount = 50

            for (i in 1..docCount) {
                val sourceMap = mapOf("name" to randomAlphaOfLength(5))
                leaderClient.index(IndexRequest(leaderIndexName).id(i.toString()).source(sourceMap), RequestOptions.DEFAULT)
            }

            assertBusy {
                val stats = leaderClient.leaderStats()
                assertThat(stats.size).isEqualTo(9)
                assertThat(stats.getValue("num_replicated_indices").toString()).isEqualTo("1")
                assertThat(stats.getValue("operations_read").toString()).isEqualTo(docCount.toString())
                assertThat(stats.getValue("operations_read_lucene").toString()).isEqualTo("0")
                assertThat(stats.getValue("operations_read_translog").toString()).isEqualTo(docCount.toString())
                assertThat(stats.containsKey("index_stats"))
            }

        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test follower stats`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        val followerIndex2 = "follower_index_2"
        val followerIndex3 = "follower_index_3"

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(
                CreateIndexRequest(leaderIndexName),
                RequestOptions.DEFAULT
        )
        assertThat(createIndexResponse.isAcknowledged).isTrue()

        try {
            followerClient.startReplication(
                    StartReplicationRequest("source", leaderIndexName, followerIndexName),
                    TimeValue.timeValueSeconds(10),
                    true
            )
            followerClient.startReplication(
                    StartReplicationRequest("source", leaderIndexName, followerIndex2),
                    TimeValue.timeValueSeconds(10),
                    true
            )
            followerClient.startReplication(
                    StartReplicationRequest("source", leaderIndexName, followerIndex3),
                    TimeValue.timeValueSeconds(10),
                    true
            )
            val docCount = 50
            for (i in 1..docCount) {
                val sourceMap = mapOf("name" to randomAlphaOfLength(5))
                leaderClient.index(IndexRequest(leaderIndexName).id(i.toString()).source(sourceMap), RequestOptions.DEFAULT)
            }

            followerClient.pauseReplication(followerIndex2)
            followerClient.stopReplication(followerIndex3)
            val stats = followerClient.followerStats()
            assertThat(stats.getValue("num_syncing_indices").toString()).isEqualTo("1")
            assertThat(stats.getValue("num_paused_indices").toString()).isEqualTo("1")
            assertThat(stats.getValue("num_failed_indices").toString()).isEqualTo("0")
            assertThat(stats.getValue("num_shard_tasks").toString()).isEqualTo("1")
            assertThat(stats.getValue("operations_written").toString()).isEqualTo("50")
            assertThat(stats.getValue("operations_read").toString()).isEqualTo("50")
            assertThat(stats.getValue("failed_read_requests").toString()).isEqualTo("0")
            assertThat(stats.getValue("failed_write_requests").toString()).isEqualTo("0")
            assertThat(stats.getValue("follower_checkpoint").toString()).isEqualTo((docCount-1).toString())
            assertThat(stats.containsKey("index_stats"))
            assertThat(stats.size).isEqualTo(16)
        } finally {
            followerClient.stopReplication(followerIndexName)
            followerClient.stopReplication(followerIndex2)
        }
    }

    fun `test that replication cannot be started on invalid indexName`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName).alias(Alias("leaderAlias")), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()

        assertValidationFailure(followerClient, "leaderIndex", followerIndexName,
            "Value leaderIndex must be lowercase")
        assertValidationFailure(followerClient, "leaderindex", "followerIndex",
            "Value followerIndex must be lowercase")

        assertValidationFailure(followerClient, "test*", followerIndexName,
            "Value test* must not contain the following characters")
        assertValidationFailure(followerClient, "test#", followerIndexName,
            "Value test# must not contain '#' or ':'")
        assertValidationFailure(followerClient, "test:", followerIndexName,
            "Value test: must not contain '#' or ':'")
        assertValidationFailure(followerClient, ".", followerIndexName,
            "Value . must not be '.' or '..'")
        assertValidationFailure(followerClient, "..", followerIndexName,
            "Value .. must not be '.' or '..'")

        assertValidationFailure(followerClient, "_leader", followerIndexName,
            "Value _leader must not start with '_' or '-' or '+'")

        assertValidationFailure(followerClient, "-leader", followerIndexName,
            "Value -leader must not start with '_' or '-' or '+'")
        assertValidationFailure(followerClient, "+leader", followerIndexName,
            "Value +leader must not start with '_' or '-' or '+'")
        assertValidationFailure(followerClient, longIndexName, followerIndexName,
            "Value $longIndexName must not be longer than ${MetadataCreateIndexService.MAX_INDEX_NAME_BYTES} bytes")
        assertValidationFailure(followerClient, ".leaderIndex", followerIndexName,
            "Value .leaderIndex must not start with '.'")
    }

    fun `test that replication is not started when start block is set`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(
                CreateIndexRequest(leaderIndexName),
                RequestOptions.DEFAULT
        )
        assertThat(createIndexResponse.isAcknowledged).isTrue()

        // Setting to add replication start block
        followerClient.updateReplicationStartBlockSetting(true)

        assertThatThrownBy { followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName),
                waitForRestore = true) }
                .isInstanceOf(ResponseException::class.java)
                .hasMessageContaining("[FORBIDDEN] Replication START block is set")

        // Remove replication start block and start replication
        followerClient.updateReplicationStartBlockSetting(false)

        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName),
                    waitForRestore = true)
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    private fun assertValidationFailure(client: RestHighLevelClient, leader: String, follower: String, errrorMsg: String) {
        assertThatThrownBy {
            client.startReplication(StartReplicationRequest("source", leader, follower))
        }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining(errrorMsg)
    }

    private fun assertEqualAliases() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        var getAliasesRequest = GetAliasesRequest().indices(followerIndexName)
        var aliasRespone = followerClient.indices().getAlias(getAliasesRequest, RequestOptions.DEFAULT)
        var followerAliases = aliasRespone.aliases.get(followerIndexName)

        aliasRespone = leaderClient.indices().getAlias(GetAliasesRequest().indices(leaderIndexName), RequestOptions.DEFAULT)
        var leaderAliases = aliasRespone.aliases.get(leaderIndexName)

        Assert.assertEquals(followerAliases, leaderAliases)
    }
}
