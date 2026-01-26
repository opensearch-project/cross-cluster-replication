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


import kotlinx.coroutines.delay
import org.opensearch.replication.IndexUtil
import org.opensearch.replication.MultiClusterAnnotations
import org.opensearch.replication.MultiClusterRestTestCase
import org.opensearch.replication.StartReplicationRequest
import org.opensearch.replication.`validate not paused status response`
import org.opensearch.replication.`validate paused status on closed index`
import org.opensearch.replication.pauseReplication
import org.opensearch.replication.replicationStatus
import org.opensearch.replication.resumeReplication
import org.opensearch.replication.`validate paused status response due to leader index deleted`
import org.opensearch.replication.`validate status syncing response`
import org.opensearch.replication.startReplication
import org.opensearch.replication.ANALYZERS_NOT_ACCESSIBLE_FOR_REMOTE_CLUSTERS
import org.opensearch.replication.SNAPSHOTS_NOT_ACCESSIBLE_FOR_REMOTE_CLUSTERS
import org.opensearch.replication.stopReplication
import org.opensearch.replication.updateReplication
import org.apache.hc.core5.http.HttpStatus
import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.apache.hc.core5.http.io.entity.EntityUtils
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.admin.cluster.repositories.put.PutRepositoryRequest
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest
import org.opensearch.action.admin.indices.alias.Alias
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.action.get.GetRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.ResponseException
import org.opensearch.client.RestHighLevelClient
import org.opensearch.client.core.CountRequest
import org.opensearch.client.indices.CloseIndexRequest
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.client.indices.GetIndexRequest
import org.opensearch.client.indices.GetMappingsRequest
import org.opensearch.client.indices.PutMappingRequest
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.MetadataCreateIndexService
import org.opensearch.common.io.PathUtils
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.IndexSettings
import org.opensearch.index.mapper.MapperService
import org.opensearch.repositories.fs.FsRepository
import org.opensearch.test.OpenSearchTestCase.assertBusy
import org.junit.Assert
import org.junit.Assume
import org.opensearch.core.xcontent.DeprecationHandler
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.replication.ReplicationPlugin.Companion.REPLICATION_INDEX_TRANSLOG_PRUNING_ENABLED_SETTING
import org.opensearch.replication.followerStats
import org.opensearch.replication.leaderStats
import org.opensearch.replication.updateReplicationStartBlockSetting
import java.nio.file.Files
import java.util.*
import java.util.concurrent.TimeUnit
import org.opensearch.bootstrap.BootstrapInfo
import org.opensearch.cluster.service.ClusterService
import org.opensearch.index.mapper.Mapping
import org.opensearch.indices.replication.common.ReplicationType
import org.opensearch.replication.util.ValidationUtil


@MultiClusterAnnotations.ClusterConfigurations(
        MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
        MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)
class StartReplicationIT: MultiClusterRestTestCase() {
    private val leaderIndexName = "leader_index"
    private val followerIndexName = "follower_index"
    private val leaderClusterPath = "testclusters/leaderCluster-"
    private val followerClusterPath = "testclusters/followCluster-"
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
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName), waitForRestore = true)
        assertBusy {
            assertThat(followerClient.indices().exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT)).isEqualTo(true)
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
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName, settings = settings), waitForRestore = true)
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
                            .indexToSettings.getOrDefault(followerIndexName, Settings.EMPTY)[IndexMetadata.SETTING_NUMBER_OF_REPLICAS]
            )
        }, 15, TimeUnit.SECONDS)
    }


    fun `test replication when _close is triggered on leader`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
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
    }


    fun `test replication when _close and _open is triggered on leader`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName), waitForRestore = true)
        followerClient.pauseReplication(followerIndexName)
        leaderClient.lowLevelClient.performRequest(Request("POST", "/" + leaderIndexName + "/_close"))
        leaderClient.lowLevelClient.performRequest(Request("POST", "/" + leaderIndexName + "/_open"))
        followerClient.resumeReplication(followerIndexName)
        var statusResp = followerClient.replicationStatus(followerIndexName)
        `validate not paused status response`(statusResp)
    }

    fun `test start replication fails when replication has already been started for the same index`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse =
            leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        followerClient.startReplication(
            StartReplicationRequest("source", leaderIndexName, followerIndexName),
            waitForRestore = true
        )
        assertThatThrownBy {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))
        }.isInstanceOf(ResponseException::class.java).hasMessageContaining(
            "Cant use same index again for replication." +
                    " Delete the index:$followerIndexName"
        )
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
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName), waitForRestore = true)
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
        assertBusy({
            Assert.assertEquals(
                leaderMappings,
                followerClient.indices().getMapping(GetMappingsRequest().indices(followerIndexName), RequestOptions.DEFAULT)
                    .mappings()[followerIndexName]
            )
        }, 30L, TimeUnit.SECONDS)
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
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName),
            waitForRestore = true)
        assertBusy {
            assertThat(followerClient.indices()
                    .exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT))
                    .isEqualTo(true)
        }
        val getSettingsRequest = GetSettingsRequest()
        getSettingsRequest.indices(followerIndexName)
        getSettingsRequest.names(IndexMetadata.SETTING_NUMBER_OF_REPLICAS)
        assertBusy({
            Assert.assertEquals(
                "0",
                followerClient.indices()
                    .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                    .indexToSettings.getOrDefault(followerIndexName, Settings.EMPTY)[IndexMetadata.SETTING_NUMBER_OF_REPLICAS]
            )
        }, 30L, TimeUnit.SECONDS)
    }

    fun `test that aliases settings are getting replicated`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName)
            .alias(Alias("leaderAlias").filter("{\"term\":{\"year\":2016}}").routing("1"))
            , RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName),
            waitForRestore = true)
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
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName),
            waitForRestore = true)
        assertBusy {
            assertThat(followerClient.indices()
                    .exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT))
                    .isEqualTo(true)
            assertThat(followerClient.indices()
                    .getSettings(GetSettingsRequest().indices(followerIndexName), RequestOptions.DEFAULT)
                    .getSetting(followerIndexName,
                            REPLICATION_INDEX_TRANSLOG_PRUNING_ENABLED_SETTING.key)
                    .isNullOrEmpty())
        }
        assertThat(leaderClient.indices()
                .getSettings(GetSettingsRequest().indices(leaderIndexName), RequestOptions.DEFAULT)
                .getSetting(leaderIndexName,
                        REPLICATION_INDEX_TRANSLOG_PRUNING_ENABLED_SETTING.key) == "true")
    }

    fun `test that translog settings are set on leader`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName),
            waitForRestore = true)
        val leaderSettings = leaderClient.indices()
                .getSettings(GetSettingsRequest().indices(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(leaderSettings.getSetting(leaderIndexName,
                        REPLICATION_INDEX_TRANSLOG_PRUNING_ENABLED_SETTING.key) == "true")
        assertThat(leaderSettings.getSetting(leaderIndexName,
                        IndexSettings.INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING.key) == "32mb")
    }

    fun `test that replication continues after removing translog settings based on retention lease`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName),
                waitForRestore = true)
        assertBusy {
            assertThat(followerClient.indices()
                    .exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT))
                    .isEqualTo(true)
        }
        // Turn-off the settings and index doc
        val settingsBuilder = Settings.builder()
                .put(REPLICATION_INDEX_TRANSLOG_PRUNING_ENABLED_SETTING.key, false)
        val settingsUpdateResponse = leaderClient.indices().putSettings(UpdateSettingsRequest(leaderIndexName)
                .settings(settingsBuilder.build()), RequestOptions.DEFAULT)
        Assert.assertEquals(settingsUpdateResponse.isAcknowledged, true)
        val sourceMap = mapOf("name" to randomAlphaOfLength(5))
        leaderClient.index(IndexRequest(leaderIndexName).id("2").source(sourceMap), RequestOptions.DEFAULT)
        assertBusy({
            followerClient.get(GetRequest(followerIndexName).id("2"), RequestOptions.DEFAULT).isExists
        }, 30L, TimeUnit.SECONDS)
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

        persistentConnectionRequest.entity = StringEntity(entityAsString, ContentType.APPLICATION_JSON)
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
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName),
            waitForRestore = true)
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
                        .indexToSettings.getOrDefault(followerIndexName, Settings.EMPTY)[IndexMetadata.SETTING_NUMBER_OF_REPLICAS]
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
        assertBusy({
            Assert.assertEquals(
                "2",
                followerClient.indices()
                    .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                    .indexToSettings.getOrDefault(followerIndexName, Settings.EMPTY)[IndexMetadata.SETTING_NUMBER_OF_REPLICAS]
            )
            assertEqualAliases()
        }, 30L, TimeUnit.SECONDS)
        // Case 2 :  Blocklisted  setting are not copied
        Assert.assertNull(followerClient.indices()
                .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                .indexToSettings.getOrDefault(followerIndexName, Settings.EMPTY).get("index.routing.allocation.enable"))
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
        assertBusy({
            Assert.assertEquals(
                "3",
                followerClient.indices()
                    .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                    .indexToSettings.getOrDefault(followerIndexName, Settings.EMPTY)[IndexMetadata.SETTING_NUMBER_OF_REPLICAS]
            )
            Assert.assertEquals(
                "10s",
                followerClient.indices()
                    .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                    .indexToSettings.getOrDefault(followerIndexName, Settings.EMPTY)["index.search.idle.after"]
            )
            Assert.assertEquals(
                "none",
                followerClient.indices()
                    .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                    .indexToSettings.getOrDefault(followerIndexName, Settings.EMPTY)["index.routing.allocation.enable"]
            )
            assertEqualAliases()
        }, 30L, TimeUnit.SECONDS)
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
        assertBusy({
            Assert.assertEquals(
                null,
                followerClient.indices()
                    .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                    .indexToSettings.getOrDefault(followerIndexName, Settings.EMPTY)["index.search.idle.after"]
            )
            assertEqualAliases()
        }, 30L, TimeUnit.SECONDS)
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
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName),
            waitForRestore = true)
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
                        .indexToSettings.getOrDefault(followerIndexName, Settings.EMPTY)[IndexMetadata.SETTING_NUMBER_OF_REPLICAS]
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
                        .indexToSettings.getOrDefault(followerIndexName, Settings.EMPTY)["index.shard.check_on_startup"]
        )
    }

    fun `test that replication fails to start when custom analyser is not present in follower`() {

        Assume.assumeFalse(ANALYZERS_NOT_ACCESSIBLE_FOR_REMOTE_CLUSTERS, checkifIntegTestRemote())

        val leaderSynonymPaths = mutableListOf<java.nio.file.Path>()
        for (i in 0 until clusterNodes(LEADER)) {
            val config = PathUtils.get(buildDir, leaderClusterPath + i, "config")
            val synonymPath = config.resolve("synonyms.txt")
            leaderSynonymPaths.add(synonymPath)
            Files.copy(javaClass.getResourceAsStream("/analyzers/synonyms.txt"), synonymPath)
        }
        try {
            val settings: Settings = Settings.builder().loadFromStream(synonymsJson, javaClass.getResourceAsStream(synonymsJson), false)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .build()
            val leaderClient = getClientForCluster(LEADER)
            val followerClient = getClientForCluster(FOLLOWER)
            try {
                val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName)
                    .settings(settings).mapping(synonymMapping, XContentType.JSON), RequestOptions.DEFAULT)
                assertThat(createIndexResponse.isAcknowledged).isTrue()
            } catch (e: Exception) {
                assumeNoException("Ignored test as analyzer setting could not be added", e)
            }
            createConnectionBetweenClusters(FOLLOWER, LEADER)
            assertThatThrownBy {
                followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))
            }.isInstanceOf(ResponseException::class.java).hasMessageContaining("resource_not_found_exception")
        } finally {
            leaderSynonymPaths.forEach { if (Files.exists(it)) Files.delete(it) }
        }
    }

    fun `test that replication starts successfully when custom analyser is present in follower`() {

        Assume.assumeFalse(ANALYZERS_NOT_ACCESSIBLE_FOR_REMOTE_CLUSTERS, checkifIntegTestRemote())

        val leaderSynonymPaths = mutableListOf<java.nio.file.Path>()
        val followerSynonymPaths = mutableListOf<java.nio.file.Path>()
        for (i in 0 until clusterNodes(LEADER)) {
            val leaderConfig = PathUtils.get(buildDir, leaderClusterPath + i, "config")
            val leaderSynonymPath = leaderConfig.resolve("synonyms.txt")
            leaderSynonymPaths.add(leaderSynonymPath)
            Files.copy(javaClass.getResourceAsStream("/analyzers/synonyms.txt"), leaderSynonymPath)
        }
        for (i in 0 until clusterNodes(FOLLOWER)) {
            val followerConfig = PathUtils.get(buildDir, followerClusterPath + i, "config")
            val followerSynonymPath = followerConfig.resolve("synonyms.txt")
            followerSynonymPaths.add(followerSynonymPath)
            Files.copy(javaClass.getResourceAsStream("/analyzers/synonyms.txt"), followerSynonymPath)
        }
        try {
            val settings: Settings = Settings.builder().loadFromStream(synonymsJson, javaClass.getResourceAsStream(synonymsJson), false)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .build()
            val leaderClient = getClientForCluster(LEADER)
            val followerClient = getClientForCluster(FOLLOWER)
            try {
                val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName)
                    .settings(settings).mapping(synonymMapping, XContentType.JSON), RequestOptions.DEFAULT)
                assertThat(createIndexResponse.isAcknowledged).isTrue()
            } catch (e: Exception) {
                assumeNoException("Ignored test as analyzer setting could not be added", e)
            }
            createConnectionBetweenClusters(FOLLOWER, LEADER)
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName),
                waitForRestore = true)
            assertBusy {
                assertThat(followerClient.indices().exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT)).isEqualTo(true)
            }
            followerClient.stopReplication(followerIndexName)
        } finally {
            leaderSynonymPaths.forEach { if (Files.exists(it)) Files.delete(it) }
            followerSynonymPaths.forEach { if (Files.exists(it)) Files.delete(it) }
        }
    }

    fun `test that replication starts successfully when custom analyser is overridden and present in follower`() {

        Assume.assumeFalse(ANALYZERS_NOT_ACCESSIBLE_FOR_REMOTE_CLUSTERS, checkifIntegTestRemote())

        val synonymFollowerFilename = "synonyms_follower.txt"
        val leaderSynonymPaths = mutableListOf<java.nio.file.Path>()
        val followerSynonymPaths = mutableListOf<java.nio.file.Path>()
        for (i in 0 until clusterNodes(LEADER)) {
            val leaderConfig = PathUtils.get(buildDir, leaderClusterPath + i, "config")
            val leaderSynonymPath = leaderConfig.resolve("synonyms.txt")
            leaderSynonymPaths.add(leaderSynonymPath)
            Files.copy(javaClass.getResourceAsStream("/analyzers/synonyms.txt"), leaderSynonymPath)
        }
        for (i in 0 until clusterNodes(FOLLOWER)) {
            val followerConfig = PathUtils.get(buildDir, followerClusterPath + i, "config")
            val followerSynonymPath = followerConfig.resolve(synonymFollowerFilename)
            followerSynonymPaths.add(followerSynonymPath)
            Files.copy(javaClass.getResourceAsStream("/analyzers/synonyms.txt"), followerSynonymPath)
        }
        try {
            val settings: Settings = Settings.builder().loadFromStream(synonymsJson, javaClass.getResourceAsStream(synonymsJson), false)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .build()
            val leaderClient = getClientForCluster(LEADER)
            val followerClient = getClientForCluster(FOLLOWER)
            try {
                val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName)
                    .settings(settings).mapping(synonymMapping, XContentType.JSON), RequestOptions.DEFAULT)
                assertThat(createIndexResponse.isAcknowledged).isTrue()
            } catch (e: Exception) {
                assumeNoException("Ignored test as analyzer setting could not be added", e)
            }
            createConnectionBetweenClusters(FOLLOWER, LEADER)
            val overriddenSettings: Settings = Settings.builder()
                .put("index.analysis.filter.my_filter.synonyms_path", synonymFollowerFilename)
                .build()
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName, overriddenSettings),
                waitForRestore = true)
            assertBusy {
                assertThat(followerClient.indices().exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT)).isEqualTo(true)
            }
            followerClient.stopReplication(followerIndexName)
        } finally {
            leaderSynonymPaths.forEach { if (Files.exists(it)) Files.delete(it) }
            followerSynonymPaths.forEach { if (Files.exists(it)) Files.delete(it) }
        }
    }

    fun `test that follower index cannot be deleted after starting replication`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName),
            waitForRestore = true)
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
        }.isInstanceOf(OpenSearchStatusException::class.java).hasMessageContaining("cluster_block_exception")
        // Close index
        assertThatThrownBy {
            followerClient.indices().close(CloseIndexRequest(followerIndexName), RequestOptions.DEFAULT)
        }.isInstanceOf(OpenSearchStatusException::class.java).hasMessageContaining("cluster_block_exception")
        // Index document
        assertThatThrownBy {
            val sourceMap = mapOf("name" to randomAlphaOfLength(5))
            followerClient.index(IndexRequest(followerIndexName).id("1").source(sourceMap), RequestOptions.DEFAULT)
        }.isInstanceOf(OpenSearchStatusException::class.java).hasMessageContaining("cluster_block_exception")
    }

    fun `test that replication gets paused if the leader index is deleted`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName),
            waitForRestore = true)
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
    }

    fun `test that write alias is stripped on follower`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        setMetadataSyncDelay() // Ensure sync happens reasonably fast
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(
            CreateIndexRequest(leaderIndexName)
                .alias(Alias("write_alias").writeIndex(true)),
            RequestOptions.DEFAULT
        )
        assertThat(createIndexResponse.isAcknowledged).isTrue()

        followerClient.startReplication(
            StartReplicationRequest("source", leaderIndexName, followerIndexName),
            waitForRestore = true
        )

        assertBusy {
            assertThat(followerClient.indices().exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT)).isEqualTo(true)
        }

        var followerAliases = followerClient.indices().getAlias(GetAliasesRequest().indices(followerIndexName), RequestOptions.DEFAULT)
        var aliasMetadata = followerAliases.aliases[followerIndexName]?.find { it.alias == "write_alias" }
        assertThat(aliasMetadata).isNotNull
        assertThat(aliasMetadata?.writeIndex()).isFalse()

        // trigger a metadata sync to ensure the alias state is persisted
        val indicesAliasesRequest = IndicesAliasesRequest()
        indicesAliasesRequest.addAliasAction(
            IndicesAliasesRequest.AliasActions.add().index(leaderIndexName).alias("new_alias")
        )
        leaderClient.indices().updateAliases(indicesAliasesRequest, RequestOptions.DEFAULT)

        TimeUnit.SECONDS.sleep(SLEEP_TIME_BETWEEN_SYNC + 2) // Wait for sync

        assertBusy {
            val aliases = followerClient.indices().getAlias(GetAliasesRequest().indices(followerIndexName), RequestOptions.DEFAULT)
            assertThat(aliases.aliases[followerIndexName]?.find { it.alias == "new_alias" }).isNotNull
        }

        followerAliases = followerClient.indices().getAlias(GetAliasesRequest().indices(followerIndexName), RequestOptions.DEFAULT)
        aliasMetadata = followerAliases.aliases[followerIndexName]?.find { it.alias == "write_alias" }
        assertThat(aliasMetadata).isNotNull
        assertThat(aliasMetadata?.writeIndex()).isFalse()
    }

    fun `test that snapshot on leader does not affect replication during bootstrap`() {

        Assume.assumeFalse(SNAPSHOTS_NOT_ACCESSIBLE_FOR_REMOTE_CLUSTERS,checkifIntegTestRemote())

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
        // Have to wait until the new operations are available to read at the leader cluster
        assertBusy({
            val stats = leaderClient.leaderStats()
            assertThat(stats.size).isEqualTo(9)
            assertThat(stats.getValue("num_replicated_indices").toString()).isEqualTo("1")
            assertThat(stats.getValue("operations_read").toString()).isEqualTo(docCount.toString())
            assertThat(stats.getValue("operations_read_lucene").toString()).isEqualTo("0")
            assertThat(stats.getValue("operations_read_translog").toString()).isEqualTo(docCount.toString())
            assertThat(stats.containsKey("index_stats"))
        }, 60L, TimeUnit.SECONDS)
    }


    fun `test follower stats`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName2 = randomAlphaOfLength(10).lowercase(Locale.ROOT)+"follower"
        val followerIndexName3 = randomAlphaOfLength(10).lowercase(Locale.ROOT)+"follower"
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(
                CreateIndexRequest(leaderIndexName),
                RequestOptions.DEFAULT
        )
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        followerClient.startReplication(
                StartReplicationRequest("source", leaderIndexName, followerIndexName),
                TimeValue.timeValueSeconds(10),
                true
        )
        followerClient.startReplication(
                StartReplicationRequest("source", leaderIndexName, followerIndexName2),
                TimeValue.timeValueSeconds(10),
                true
        )
        followerClient.startReplication(
                StartReplicationRequest("source", leaderIndexName, followerIndexName3),
                TimeValue.timeValueSeconds(10),
                true
        )
        val docCount = 50
        for (i in 1..docCount) {
            val sourceMap = mapOf("name" to randomAlphaOfLength(5))
            leaderClient.index(IndexRequest(leaderIndexName).id(i.toString()).source(sourceMap), RequestOptions.DEFAULT)
        }
        followerClient.pauseReplication(followerIndexName2)
        followerClient.stopReplication(followerIndexName3)
        var stats = followerClient.followerStats()
        assertThat(stats.getValue("num_syncing_indices").toString()).isEqualTo("1")
        assertThat(stats.getValue("num_paused_indices").toString()).isEqualTo("1")
        assertThat(stats.getValue("num_failed_indices").toString()).isEqualTo("0")
        assertThat(stats.getValue("num_shard_tasks").toString()).isEqualTo("1")
        assertBusy({
            stats = followerClient.followerStats()
            assertThat(stats.getValue("operations_written").toString()).isEqualTo("50")
        }, 60, TimeUnit.SECONDS)
        assertThat(stats.getValue("operations_read").toString()).isEqualTo("50")
        assertThat(stats.getValue("failed_read_requests").toString()).isEqualTo("0")
        assertThat(stats.getValue("failed_write_requests").toString()).isEqualTo("0")
        assertThat(stats.getValue("follower_checkpoint").toString()).isEqualTo((docCount-1).toString())
        assertThat(stats.containsKey("index_stats"))
        assertThat(stats.size).isEqualTo(16)
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
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName),
                    waitForRestore = true)
    }

    fun `test start replication invalid settings`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        val settings = Settings.builder()
            .put("index.data_path", "/random-path/invalid-setting")
            .build()
        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName, settings = settings))
        } catch (e: ResponseException) {
            Assert.assertEquals(400, e.response.statusLine.statusCode)
            Assert.assertTrue(e.message!!.contains("Validation Failed: 1: custom path [/random-path/invalid-setting] is not a sub-path of path.shared_data"))
        }
    }

    fun `test that replication is not started when all primary shards are not in active state`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        // Exclude leader cluster nodes to stop assignment for the new shards
        excludeAllClusterNodes(LEADER)
        try{
            leaderClient.indices().create(
                    CreateIndexRequest(leaderIndexName),
                    RequestOptions.DEFAULT
            )
        } catch(_: Exception) {
            // Index creation
        }
        // Index should be present (although shards will not assigned).
        assertBusy {
            assertThat(leaderClient.indices().exists(GetIndexRequest(leaderIndexName), RequestOptions.DEFAULT)).isEqualTo(true)
        }
        // start repilcation should fail as the shards are not active on the leader cluster
        assertThatThrownBy { followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName),
                waitForRestore = true) }
                .isInstanceOf(ResponseException::class.java)
                .hasMessageContaining("Primary shards in the Index[source:${leaderIndexName}] are not active")
    }

    fun `test that wait_for_active_shards setting is set on leader and not on follower`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val settings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), Integer.toString(2))
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
            TimeUnit.SECONDS.sleep(SLEEP_TIME_BETWEEN_SYNC)

            // Verify the setting on leader
            val getLeaderSettingsRequest = GetSettingsRequest()
            getLeaderSettingsRequest.indices(leaderIndexName)
            getLeaderSettingsRequest.includeDefaults(true)

            assertBusy ({
                Assert.assertEquals(
                        "2",
                        leaderClient.indices()
                                .getSettings(getLeaderSettingsRequest, RequestOptions.DEFAULT)
                                .indexToSettings.getOrDefault(leaderIndexName, Settings.EMPTY)[IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey()]
                )
            }, 15, TimeUnit.SECONDS)

            // Verify that the setting is not updated on follower and follower has default value of the setting
            val getSettingsRequest = GetSettingsRequest()
            getSettingsRequest.indices(followerIndexName)
            getSettingsRequest.includeDefaults(true)

            assertBusy ({
                Assert.assertEquals(
                        "1",
                        followerClient.indices()
                                .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                                .getSetting(followerIndexName, IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS.key)
                )
            }, 15, TimeUnit.SECONDS)
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test that wait_for_active_shards setting is updated on leader and not on follower`() {
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
            TimeUnit.SECONDS.sleep(SLEEP_TIME_BETWEEN_SYNC)

            //Use Update API
            val settingsBuilder = Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), Integer.toString(2))

            val settingsUpdateResponse = leaderClient.indices().putSettings(UpdateSettingsRequest(leaderIndexName)
                    .settings(settingsBuilder.build()), RequestOptions.DEFAULT)
            Assert.assertEquals(settingsUpdateResponse.isAcknowledged, true)

            TimeUnit.SECONDS.sleep(SLEEP_TIME_BETWEEN_SYNC)

            // Verify the setting on leader
            val getLeaderSettingsRequest = GetSettingsRequest()
            getLeaderSettingsRequest.indices(leaderIndexName)
            getLeaderSettingsRequest.includeDefaults(true)

            assertBusy ({
                Assert.assertEquals(
                        "2",
                        leaderClient.indices()
                                .getSettings(getLeaderSettingsRequest, RequestOptions.DEFAULT)
                                .indexToSettings.getOrDefault(leaderIndexName, Settings.EMPTY)[IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey()]
                )
            }, 15, TimeUnit.SECONDS)


            val getSettingsRequest = GetSettingsRequest()
            getSettingsRequest.indices(followerIndexName)
            getSettingsRequest.includeDefaults(true)

            assertBusy ({
                Assert.assertEquals(
                        "1",
                        followerClient.indices()
                                .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                                .getSetting(followerIndexName, IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS.key)
                )
            }, 15, TimeUnit.SECONDS)
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test that wait_for_active_shards setting is updated on follower through start replication api`() {

        Assume.assumeTrue("Ignore this test if clusters dont have multiple nodes as this test reles on wait_for_active_shards",
            isMultiNodeClusterConfiguration(LEADER, FOLLOWER))

        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()

        val settings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), Integer.toString(2))
                .build()
        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName, settings = settings))
            assertBusy {
                assertThat(followerClient.indices()
                        .exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT))
                        .isEqualTo(true)
            }
            TimeUnit.SECONDS.sleep(SLEEP_TIME_BETWEEN_SYNC)

            val getSettingsRequest = GetSettingsRequest()
            getSettingsRequest.indices(followerIndexName)
            getSettingsRequest.includeDefaults(true)
            assertBusy ({
                Assert.assertEquals(
                        "2",
                        followerClient.indices()
                                .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                                .indexToSettings.getOrDefault(followerIndexName, Settings.EMPTY)[IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey()]
                )
            }, 15, TimeUnit.SECONDS)
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }
    fun `test that follower index mapping updates when leader index gets multi-field mapping`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        var putMappingRequest = PutMappingRequest(leaderIndexName)
        putMappingRequest.source("{\"properties\":{\"field1\":{\"type\":\"text\"}}}", XContentType.JSON)
        leaderClient.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT)
        val sourceMap = mapOf("field1" to randomAlphaOfLength(5))
        leaderClient.index(IndexRequest(leaderIndexName).id("1").source(sourceMap), RequestOptions.DEFAULT)
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName),
            waitForRestore = true)
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
        putMappingRequest = PutMappingRequest(leaderIndexName)
        putMappingRequest.source("{\"properties\":{\"field1\":{\"type\":\"text\",\"fields\":{\"field2\":{\"type\":\"text\",\"analyzer\":\"standard\"},\"field3\":{\"type\":\"text\",\"analyzer\":\"standard\"}}}}}",XContentType.JSON)
        leaderClient.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT)
        val leaderMappings = leaderClient.indices().getMapping(GetMappingsRequest().indices(leaderIndexName), RequestOptions.DEFAULT)
            .mappings()[leaderIndexName]
        TimeUnit.MINUTES.sleep(2)
        Assert.assertEquals(
            leaderMappings,
            followerClient.indices().getMapping(GetMappingsRequest().indices(followerIndexName), RequestOptions.DEFAULT)
                .mappings()[followerIndexName]
        )
    }

    fun `test that follower index mapping does not update when only new fields are added but not respective docs in leader index`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        var putMappingRequest = PutMappingRequest(leaderIndexName)
        putMappingRequest.source("{\"properties\":{\"name\":{\"type\":\"text\"}}}", XContentType.JSON)
        leaderClient.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT)
        val sourceMap = mapOf("name" to randomAlphaOfLength(5))
        leaderClient.index(IndexRequest(leaderIndexName).id("1").source(sourceMap), RequestOptions.DEFAULT)
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName),
            waitForRestore = true)
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
        putMappingRequest = PutMappingRequest(leaderIndexName)
        putMappingRequest.source("{\"properties\":{\"name\":{\"type\":\"text\"},\"age\":{\"type\":\"integer\"}}}",XContentType.JSON)
        leaderClient.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT)
        val leaderMappings = leaderClient.indices().getMapping(GetMappingsRequest().indices(leaderIndexName), RequestOptions.DEFAULT)
            .mappings()[leaderIndexName]
        TimeUnit.MINUTES.sleep(2)
        Assert.assertNotEquals(
            leaderMappings,
            followerClient.indices().getMapping(GetMappingsRequest().indices(followerIndexName), RequestOptions.DEFAULT)
                .mappings()[followerIndexName]
        )
    }

    fun `test operations are fetched from lucene when leader is in mixed mode`() {

        val leaderClient = getClientForCluster(LEADER)
        val followerClient = getClientForCluster(FOLLOWER)

        // create index on leader cluster
        val settings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .build()
        val createIndexResponse = leaderClient.indices().create(
                CreateIndexRequest(leaderIndexName).settings(settings),
                RequestOptions.DEFAULT
        )
        assertThat(createIndexResponse.isAcknowledged).isTrue()

        // Update leader cluster settings to enable mixed mode and set migration direction to remote_store
        val leaderClusterUpdateSettingsRequest = Request("PUT", "_cluster/settings")
        val entityAsString = """
                        {
                          "persistent": {
                             "cluster.remote_store.compatibility_mode": "mixed",
                             "cluster.migration.direction" : "remote_store"
                          }
                        }""".trimMargin()

        leaderClusterUpdateSettingsRequest.entity = StringEntity(entityAsString,ContentType.APPLICATION_JSON)
        val updateSettingResponse = leaderClient.lowLevelClient.performRequest(leaderClusterUpdateSettingsRequest)
        assertEquals(HttpStatus.SC_OK.toLong(), updateSettingResponse.statusLine.statusCode.toLong())

        //create connection and start replication
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        followerClient.startReplication(
                StartReplicationRequest("source", leaderIndexName, followerIndexName),
                TimeValue.timeValueSeconds(10),
                true
        )

        //Index documents on leader index
        val docCount = 50
        for (i in 1..docCount) {
            val sourceMap = mapOf("name" to randomAlphaOfLength(5))
            leaderClient.index(IndexRequest(leaderIndexName).id(i.toString()).source(sourceMap), RequestOptions.DEFAULT)
        }

        // Verify that all the documents are replicated to follower index and are fetched from lucene
        assertBusy({
            val stats = leaderClient.leaderStats()
            assertThat(stats.size).isEqualTo(9)
            assertThat(stats.getValue("num_replicated_indices").toString()).isEqualTo("1")
            assertThat(stats.getValue("operations_read").toString()).isEqualTo(docCount.toString())
            assertThat(stats.getValue("operations_read_lucene").toString()).isEqualTo(docCount.toString())
            assertThat(stats.getValue("operations_read_translog").toString()).isEqualTo("0")
            assertThat(stats.containsKey("index_stats"))
        }, 60L, TimeUnit.SECONDS)
    }

    private fun excludeAllClusterNodes(clusterName: String) {
        val transientSettingsRequest = Request("PUT", "_cluster/settings")
        // Get IPs directly from the cluster to handle all cases - single node cluster, multi node cluster and remote test cluster.
        val excludeIps = getClusterNodeIPs(clusterName)
        val entityAsString = """
                        {
                          "transient": {
                             "cluster.routing.allocation.exclude._ip": "${excludeIps.joinToString()}"
                          }
                        }""".trimMargin()
        transientSettingsRequest.entity = StringEntity(entityAsString, ContentType.APPLICATION_JSON)
        val transientSettingsResponse = getNamedCluster(clusterName).lowLevelClient.performRequest(transientSettingsRequest)
        assertEquals(HttpStatus.SC_OK.toLong(), transientSettingsResponse.statusLine.statusCode.toLong())
    }

    private fun getClusterNodeIPs(clusterName: String): List<String> {
        val clusterClient = getNamedCluster(clusterName).lowLevelClient
        val nodesRequest = Request("GET", "_cat/nodes?format=json")
        val nodesResponse =  EntityUtils.toString(clusterClient.performRequest(nodesRequest).entity)
        val nodeIPs = arrayListOf<String>()
        val parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, nodesResponse)
        parser.list().forEach {
            it as Map<*, *>
            nodeIPs.add(it["ip"] as String)
        }
        return nodeIPs
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
        val getAliasesRequest = GetAliasesRequest().indices(followerIndexName)
        var aliasResponse = followerClient.indices().getAlias(getAliasesRequest, RequestOptions.DEFAULT)
        val followerAliases = aliasResponse.aliases.get(followerIndexName)
        aliasResponse = leaderClient.indices().getAlias(GetAliasesRequest().indices(leaderIndexName), RequestOptions.DEFAULT)
        val leaderAliases = aliasResponse.aliases.get(leaderIndexName)

        if (followerAliases == null || leaderAliases == null) {
            Assert.fail("Aliases are null. Leader: $leaderAliases, Follower: $followerAliases")
        }
        if (followerAliases!!.size != leaderAliases!!.size) {
            Assert.fail("Alias count mismatch. Leader: ${leaderAliases.size}, Follower: ${followerAliases.size}")
        }

        for (leader in leaderAliases) {
            val follower = followerAliases.find { it.alias() == leader.alias() }
            Assert.assertNotNull("Follower missing alias ${leader.alias()}", follower)

            Assert.assertEquals("Filter mismatch for ${leader.alias()}", leader.filter(), follower!!.filter())
            Assert.assertEquals("IndexRouting mismatch for ${leader.alias()}", leader.indexRouting(), follower.indexRouting())
            Assert.assertEquals("SearchRouting mismatch for ${leader.alias()}", leader.searchRouting(), follower.searchRouting())
            Assert.assertEquals("IsHidden mismatch for ${leader.alias()}", leader.isHidden(), follower.isHidden())

            val leaderWrite = leader.writeIndex()
            val followerWrite = follower.writeIndex()

            if (leaderWrite == true) {
                Assert.assertFalse("Follower alias ${follower.alias()} should have writeIndex=false when leader is true", followerWrite == true)
            } else {
                Assert.assertEquals("WriteIndex mismatch for ${leader.alias()} (Leader: $leaderWrite)", leaderWrite, followerWrite)
            }
        }
    }
}

