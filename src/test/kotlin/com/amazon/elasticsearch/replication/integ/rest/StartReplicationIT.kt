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
import org.apache.http.HttpStatus
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.elasticsearch.action.admin.indices.alias.Alias
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Request
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.ResponseException
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.client.indices.GetMappingsRequest
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.IndexSettings
import org.elasticsearch.test.ESTestCase.assertBusy
import org.junit.Assert
import java.util.concurrent.TimeUnit


@MultiClusterAnnotations.ClusterConfigurations(
        MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
        MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)
class StartReplicationIT: MultiClusterRestTestCase() {
    private val leaderIndexName = "leader_index"
    private val followerIndexName = "follower_index"

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
                " Either close or delete the index:$followerIndexName")
    }

    fun `test start replication fails when remote cluster alias does not exist`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        assertThatThrownBy {
            followerClient.startReplication(StartReplicationRequest("doesNotExist", leaderIndexName, followerIndexName))
        }.isInstanceOf(ResponseException::class.java).hasMessageContaining("{\"error\":{\"root_cause\":[{\"type\":\"no_such_remote_cluster_exception\"," +
                "\"reason\":\"no such remote cluster: [doesNotExist]\"}]")
    }

    fun `test start replication fails when index does not exist`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        assertThatThrownBy {
            followerClient.startReplication(StartReplicationRequest("source", "doesNotExist", followerIndexName))
        }.isInstanceOf(ResponseException::class.java).hasMessageContaining("{\"error\":{\"root_cause\":[{\"type\":\"index_not_found_exception\"," +
                "\"reason\":\"no such index [doesNotExist]\"")
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

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName).alias(Alias("leaderAlias")), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))
            assertBusy {
                assertThat(followerClient.indices()
                        .exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT))
                        .isEqualTo(true)
            }
            Assert.assertEquals(
                    leaderClient.indices().getAlias(GetAliasesRequest().indices(leaderIndexName),
                            RequestOptions.DEFAULT).aliases[leaderIndexName],
                    followerClient.indices().getAlias(GetAliasesRequest().indices(followerIndexName),
                            RequestOptions.DEFAULT).aliases[followerIndexName]
            )
        } finally {
            followerClient.stopReplication(followerIndexName)
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
                        .getSetting(followerIndexName, IndexSettings.INDEX_TRANSLOG_RETENTION_LEASE_PRUNING_ENABLED_SETTING.key)
                        .isNullOrEmpty())
            }

            assertThat(leaderClient.indices()
                    .getSettings(GetSettingsRequest().indices(leaderIndexName), RequestOptions.DEFAULT)
                    .getSetting(leaderIndexName, IndexSettings.INDEX_TRANSLOG_RETENTION_LEASE_PRUNING_ENABLED_SETTING.key) == "true")

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
            val settingsBuilder = Settings.builder().put(IndexSettings.INDEX_TRANSLOG_RETENTION_LEASE_PRUNING_ENABLED_SETTING.key, false)
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
                    .alias("alias1")
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
