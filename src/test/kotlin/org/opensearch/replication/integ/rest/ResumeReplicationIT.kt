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
import org.opensearch.replication.`validate aggregated paused status response`
import org.opensearch.replication.`validate not paused status aggregated response`
import org.opensearch.replication.`validate not paused status response`
import org.opensearch.replication.`validate paused status response`
import org.opensearch.replication.`validate status syncing aggregated response`
import org.opensearch.replication.`validate status syncing response`
import org.opensearch.replication.pauseReplication
import org.opensearch.replication.replicationStatus
import org.opensearch.replication.resumeReplication
import org.opensearch.replication.startReplication
import org.opensearch.replication.stopReplication
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.opensearch.action.DocWriteResponse
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.admin.indices.open.OpenIndexRequest
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.client.RequestOptions
import org.opensearch.client.ResponseException
import org.opensearch.client.indices.CloseIndexRequest
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.client.indices.GetMappingsRequest
import org.opensearch.common.io.PathUtils
import org.opensearch.common.settings.Settings
import org.junit.Assert
import org.junit.Assume
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import org.opensearch.replication.ANALYZERS_NOT_ACCESSIBLE_FOR_REMOTE_CLUSTERS

@MultiClusterAnnotations.ClusterConfigurations(
        MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
        MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)
class ResumeReplicationIT: MultiClusterRestTestCase() {
    private val leaderIndexName = "leader_index"
    private val followerIndexName = "resumed_index"
    private val leaderClusterPath = "testclusters/leaderCluster-"
    private val followerClusterPath = "testclusters/followCluster-"
    private val buildDir = System.getProperty("build.dir")
    private val synonymsJson = "/analyzers/synonym_setting.json"

    fun `test pause and resume replication in following state and empty index`() {
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
        var statusResp = followerClient.replicationStatus(followerIndexName)
        `validate paused status response`(statusResp)
        statusResp = followerClient.replicationStatus(followerIndexName,false)
        `validate aggregated paused status response`(statusResp)
        followerClient.resumeReplication(followerIndexName)
    }


    fun `test resume without pause `() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName), waitForRestore = true)
        assertThatThrownBy {
            var statusResp = followerClient.replicationStatus(followerIndexName)
            `validate status syncing response`(statusResp)
            statusResp = followerClient.replicationStatus(followerIndexName,false)
            `validate status syncing aggregated response`(statusResp)
            followerClient.resumeReplication(followerIndexName)
            statusResp = followerClient.replicationStatus(followerIndexName)
            `validate not paused status response`(statusResp)
            statusResp = followerClient.replicationStatus(followerIndexName,false)
            `validate not paused status aggregated response`(statusResp)
        }.isInstanceOf(ResponseException::class.java)
                .hasMessageContaining("Replication on Index ${followerIndexName} is already running")
    }

    fun `test resume without retention lease`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        var createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName), waitForRestore = true)
        followerClient.pauseReplication(followerIndexName)
        // If we delete the existing index and recreate the index with same name, retention leases should be lost
        val deleteIndexResponse = leaderClient.indices().delete(DeleteIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(deleteIndexResponse.isAcknowledged).isTrue()
        createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        assertThatThrownBy {
            followerClient.resumeReplication(followerIndexName)
        }.isInstanceOf(ResponseException::class.java)
                .hasMessageContaining("Retention lease doesn't exist. Replication can't be resumed for $followerIndexName")
    }

    fun `test pause and resume replication amid leader index close and open`() {
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
        leaderClient.indices().close(CloseIndexRequest(leaderIndexName), RequestOptions.DEFAULT);
        leaderClient.indices().open(OpenIndexRequest(leaderIndexName), RequestOptions.DEFAULT);
        followerClient.resumeReplication(followerIndexName)
        //Update mapping post resume assert
        val sourceMap : MutableMap<String, String> = HashMap()
        sourceMap["x"] = "y"
        val indexResponse = leaderClient.index(IndexRequest(leaderIndexName).id("2").source(sourceMap), RequestOptions.DEFAULT)
        assertThat(indexResponse.result).isIn(DocWriteResponse.Result.CREATED, DocWriteResponse.Result.UPDATED)
        assertBusy ({
            Assert.assertEquals(
                    leaderClient.indices().getMapping(GetMappingsRequest().indices(leaderIndexName), RequestOptions.DEFAULT)
                            .mappings()[leaderIndexName],
                    followerClient.indices().getMapping(GetMappingsRequest().indices(followerIndexName), RequestOptions.DEFAULT)
                            .mappings()[followerIndexName]
            )
        }, 60, TimeUnit.SECONDS)
    }

    fun `test pause and resume replication amid index close`() {
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
        leaderClient.indices().close(CloseIndexRequest(leaderIndexName), RequestOptions.DEFAULT);
        assertThatThrownBy {
            followerClient.resumeReplication(followerIndexName)
        }.isInstanceOf(ResponseException::class.java)
                .hasMessageContaining("closed")
    }

    fun `test that replication fails to resume when custom analyser is not present in follower`() {

        Assume.assumeFalse(ANALYZERS_NOT_ACCESSIBLE_FOR_REMOTE_CLUSTERS, checkifIntegTestRemote())

        val synonyms = javaClass.getResourceAsStream("/analyzers/synonyms.txt")
        val leaderSynonymPaths = mutableListOf<java.nio.file.Path>()
        for (i in 0 until clusterNodes(LEADER)) {
            val config = PathUtils.get(buildDir, leaderClusterPath + i, "config")
            val synonymPath = config.resolve("synonyms.txt")
            leaderSynonymPaths.add(synonymPath)
            Files.copy(javaClass.getResourceAsStream("/analyzers/synonyms.txt"), synonymPath)
        }
        val leaderClient = getClientForCluster(LEADER)
        val followerClient = getClientForCluster(FOLLOWER)
        try {
            val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
            assertThat(createIndexResponse.isAcknowledged).isTrue()
            createConnectionBetweenClusters(FOLLOWER, LEADER)
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName), waitForRestore = true)
            followerClient.pauseReplication(followerIndexName)
            leaderClient.indices().close(CloseIndexRequest(leaderIndexName), RequestOptions.DEFAULT);
            val settings: Settings = Settings.builder().loadFromStream(synonymsJson, javaClass.getResourceAsStream(synonymsJson), false)
                .build()
            try {
                leaderClient.indices().putSettings(UpdateSettingsRequest(leaderIndexName).settings(settings), RequestOptions.DEFAULT)
            } catch (e: Exception) {
                assumeNoException("Ignored test as analyzer setting could not be added", e)
            }
            leaderClient.indices().open(OpenIndexRequest(leaderIndexName), RequestOptions.DEFAULT);
            assertThatThrownBy {
                followerClient.resumeReplication(followerIndexName)
            }.isInstanceOf(ResponseException::class.java).hasMessageContaining("resource_not_found_exception")
        } finally {
            leaderSynonymPaths.forEach { if (Files.exists(it)) Files.delete(it) }
        }
    }

    fun `test that replication resumes when custom analyser is present in follower`() {

        Assume.assumeFalse(ANALYZERS_NOT_ACCESSIBLE_FOR_REMOTE_CLUSTERS, checkifIntegTestRemote())

        val synonymFilename = "synonyms.txt"
        val leaderSynonymPaths = mutableListOf<java.nio.file.Path>()
        val followerSynonymPaths = mutableListOf<java.nio.file.Path>()
        for (i in 0 until clusterNodes(LEADER)) {
            val config = PathUtils.get(buildDir, leaderClusterPath + i, "config")
            val synonymPath = config.resolve(synonymFilename)
            leaderSynonymPaths.add(synonymPath)
            Files.copy(javaClass.getResourceAsStream("/analyzers/synonyms.txt"), synonymPath)
        }
        val leaderClient = getClientForCluster(LEADER)
        val followerClient = getClientForCluster(FOLLOWER)
        try {
            val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
            assertThat(createIndexResponse.isAcknowledged).isTrue()
            createConnectionBetweenClusters(FOLLOWER, LEADER)
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName), waitForRestore = true)
            followerClient.pauseReplication(followerIndexName)
            leaderClient.indices().close(CloseIndexRequest(leaderIndexName), RequestOptions.DEFAULT);
            for (i in 0 until clusterNodes(FOLLOWER)) {
                val followerConfig = PathUtils.get(buildDir, followerClusterPath + i, "config")
                val followerSynonymPath = followerConfig.resolve(synonymFilename)
                followerSynonymPaths.add(followerSynonymPath)
                Files.copy(javaClass.getResourceAsStream("/analyzers/synonyms.txt"), followerSynonymPath)
            }
            val settings: Settings = Settings.builder().loadFromStream(synonymsJson, javaClass.getResourceAsStream(synonymsJson), false)
                .build()
            try {
                leaderClient.indices().putSettings(UpdateSettingsRequest(leaderIndexName).settings(settings), RequestOptions.DEFAULT)
            } catch (e: Exception) {
                assumeNoException("Ignored test as analyzer setting could not be added", e)
            }
            leaderClient.indices().open(OpenIndexRequest(leaderIndexName), RequestOptions.DEFAULT);
            followerClient.resumeReplication(followerIndexName)
            var statusResp = followerClient.replicationStatus(followerIndexName)
            `validate status syncing response`(statusResp)
            followerClient.stopReplication(followerIndexName)
        } finally {
            leaderSynonymPaths.forEach { if (Files.exists(it)) Files.delete(it) }
            followerSynonymPaths.forEach { if (Files.exists(it)) Files.delete(it) }
        }
    }

    fun `test that replication resumes when custom analyser is overridden and present in follower`() {

        Assume.assumeFalse(ANALYZERS_NOT_ACCESSIBLE_FOR_REMOTE_CLUSTERS, checkifIntegTestRemote())

        val followerSynonymFilename = "synonyms_follower.txt"
        val leaderSynonymPaths = mutableListOf<java.nio.file.Path>()
        val leaderNewSynonymPaths = mutableListOf<java.nio.file.Path>()
        val followerSynonymPaths = mutableListOf<java.nio.file.Path>()
        for (i in 0 until clusterNodes(LEADER)) {
            val config = PathUtils.get(buildDir, leaderClusterPath + i, "config")
            val synonymPath = config.resolve("synonyms.txt")
            leaderSynonymPaths.add(synonymPath)
            leaderNewSynonymPaths.add(config.resolve("synonyms_new.txt"))
            Files.copy(javaClass.getResourceAsStream("/analyzers/synonyms.txt"), synonymPath)
        }
        for (i in 0 until clusterNodes(FOLLOWER)) {
            val followerConfig = PathUtils.get(buildDir, followerClusterPath + i, "config")
            val followerSynonymPath = followerConfig.resolve(followerSynonymFilename)
            followerSynonymPaths.add(followerSynonymPath)
            Files.copy(javaClass.getResourceAsStream("/analyzers/synonyms.txt"), followerSynonymPath)
        }
        val leaderClient = getClientForCluster(LEADER)
        val followerClient = getClientForCluster(FOLLOWER)
        try {
            var settings: Settings = Settings.builder().loadFromStream(synonymsJson, javaClass.getResourceAsStream(synonymsJson), false)
                .build()
            try {
                val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName).settings(settings), RequestOptions.DEFAULT)
                assertThat(createIndexResponse.isAcknowledged).isTrue()
            } catch (e: Exception) {
                assumeNoException("Ignored test as analyzer setting could not be added", e)
            }
            createConnectionBetweenClusters(FOLLOWER, LEADER)
            val overriddenSettings: Settings = Settings.builder()
                .put("index.analysis.filter.my_filter.synonyms_path", followerSynonymFilename)
                .build()
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName, overriddenSettings), waitForRestore = true)
            followerClient.pauseReplication(followerIndexName)
            leaderClient.indices().close(CloseIndexRequest(leaderIndexName), RequestOptions.DEFAULT);
            for (newSynonymPath in leaderNewSynonymPaths) {
                Files.copy(javaClass.getResourceAsStream("/analyzers/synonyms.txt"), newSynonymPath)
            }
            settings = Settings.builder()
                .put("index.analysis.filter.my_filter.synonyms_path", "synonyms_new.txt")
                .build()
            try {
                leaderClient.indices().putSettings(UpdateSettingsRequest(leaderIndexName).settings(settings), RequestOptions.DEFAULT)
            } catch (e: Exception) {
                assumeNoException("Ignored test as analyzer setting could not be added", e)
            }
            leaderClient.indices().open(OpenIndexRequest(leaderIndexName), RequestOptions.DEFAULT);
            followerClient.resumeReplication(followerIndexName)
            var statusResp = followerClient.replicationStatus(followerIndexName)
            `validate status syncing response`(statusResp)
            followerClient.stopReplication(followerIndexName)
        } finally {
            leaderSynonymPaths.forEach { if (Files.exists(it)) Files.delete(it) }
            followerSynonymPaths.forEach { if (Files.exists(it)) Files.delete(it) }
            leaderNewSynonymPaths.forEach { if (Files.exists(it)) Files.delete(it) }
        }
    }
}
