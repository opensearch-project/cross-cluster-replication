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
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.elasticsearch.action.DocWriteResponse
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.ResponseException
import org.elasticsearch.client.indices.CloseIndexRequest
import org.elasticsearch.client.indices.CreateIndexRequest
import org.junit.Assert
import org.elasticsearch.client.indices.GetMappingsRequest
import org.elasticsearch.common.io.PathUtils
import org.elasticsearch.common.settings.Settings
import java.nio.file.Files


@MultiClusterAnnotations.ClusterConfigurations(
        MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
        MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)
class ResumeReplicationIT: MultiClusterRestTestCase() {
    private val leaderIndexName = "leader_index"
    private val followerIndexName = "resumed_index"
    private val leaderClusterPath = "testclusters/leaderCluster-0"
    private val followerClusterPath = "testclusters/followCluster-0"
    private val buildDir = System.getProperty("build.dir")
    private val synonymsJson = "/analyzers/synonym_setting.json"

    fun `test pause and resume replication in following state and empty index`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

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
            `validate paused status resposne`(statusResp)
            statusResp = followerClient.replicationStatus(followerIndexName,false)
            `validate aggregated paused status resposne`(statusResp)
            followerClient.resumeReplication(followerIndexName)
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }


    fun `test resume without pause `() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName), waitForRestore = true)

            assertThatThrownBy {
                var statusResp = followerClient.replicationStatus(followerIndexName)
                `validate status syncing resposne`(statusResp)
                statusResp = followerClient.replicationStatus(followerIndexName,false)
                `validate status syncing aggregated resposne`(statusResp)
                followerClient.resumeReplication(followerIndexName)
                statusResp = followerClient.replicationStatus(followerIndexName)
                `validate not paused status resposne`(statusResp)
                statusResp = followerClient.replicationStatus(followerIndexName,false)
                `validate not paused status aggregated resposne`(statusResp)
            }.isInstanceOf(ResponseException::class.java)
                    .hasMessageContaining("Replication on Index ${followerIndexName} is already running")
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test resume without retention lease`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        var createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
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
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test pause and resume replication amid leader index close and open`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
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
            Thread.sleep(1000)
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

    fun `test pause and resume replication amid index close`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
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
        } finally {
            try {
                followerClient.stopReplication(followerIndexName)
            } catch (e: Exception) {
                // DO nothing
            }
        }
    }

    fun `test that replication fails to resume when custom analyser is not present in follower`() {
        val synonyms = javaClass.getResourceAsStream("/analyzers/synonyms.txt")
        val config = PathUtils.get(buildDir, leaderClusterPath, "config")
        val synonymPath = config.resolve("synonyms.txt")
        val leaderClient = getClientForCluster(LEADER)
        val followerClient = getClientForCluster(FOLLOWER)
        try {
            Files.copy(synonyms, synonymPath)

            val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
            assertThat(createIndexResponse.isAcknowledged).isTrue()

            createConnectionBetweenClusters(FOLLOWER, LEADER)
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName), waitForRestore = true)
            followerClient.pauseReplication(followerIndexName)
            leaderClient.indices().close(CloseIndexRequest(leaderIndexName), RequestOptions.DEFAULT);
            val settings: Settings = Settings.builder().loadFromStream(synonymsJson, javaClass.getResourceAsStream(synonymsJson), false)
                .build()

            leaderClient.indices().putSettings(UpdateSettingsRequest(leaderIndexName).settings(settings), RequestOptions.DEFAULT)
            leaderClient.indices().open(OpenIndexRequest(leaderIndexName), RequestOptions.DEFAULT);

            assertThatThrownBy {
                followerClient.resumeReplication(followerIndexName)
            }.isInstanceOf(ResponseException::class.java).hasMessageContaining("resource_not_found_exception")
        } finally {
            if (Files.exists(synonymPath)) {
                Files.delete(synonymPath)
            }
            try {
                followerClient.stopReplication(followerIndexName)
            } catch (e: Exception) {
                // DO nothing
            }
        }
    }

    fun `test that replication resumes when custom analyser is present in follower`() {
        val synonyms = javaClass.getResourceAsStream("/analyzers/synonyms.txt")
        val config = PathUtils.get(buildDir, leaderClusterPath, "config")
        val synonymFilename = "synonyms.txt"
        val synonymPath = config.resolve(synonymFilename)
        val followerConfig = PathUtils.get(buildDir, followerClusterPath, "config")
        val followerSynonymPath = followerConfig.resolve(synonymFilename)
        val leaderClient = getClientForCluster(LEADER)
        val followerClient = getClientForCluster(FOLLOWER)
        try {
            Files.copy(synonyms, synonymPath)

            val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
            assertThat(createIndexResponse.isAcknowledged).isTrue()

            createConnectionBetweenClusters(FOLLOWER, LEADER)

            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName), waitForRestore = true)
            followerClient.pauseReplication(followerIndexName)
            leaderClient.indices().close(CloseIndexRequest(leaderIndexName), RequestOptions.DEFAULT);
            Files.copy(synonyms, followerSynonymPath)
            val settings: Settings = Settings.builder().loadFromStream(synonymsJson, javaClass.getResourceAsStream(synonymsJson), false)
                .build()

            leaderClient.indices().putSettings(UpdateSettingsRequest(leaderIndexName).settings(settings), RequestOptions.DEFAULT)
            leaderClient.indices().open(OpenIndexRequest(leaderIndexName), RequestOptions.DEFAULT);

            followerClient.resumeReplication(followerIndexName)
            var statusResp = followerClient.replicationStatus(followerIndexName)
            `validate status syncing resposne`(statusResp)
        } finally {
            if (Files.exists(synonymPath)) {
                Files.delete(synonymPath)
            }
            if (Files.exists(followerSynonymPath)) {
                Files.delete(followerSynonymPath)
            }
            try {
                followerClient.stopReplication(followerIndexName)
            } catch (e: Exception) {
                // DO nothing
            }
        }
    }

    fun `test that replication resumes when custom analyser is overridden and present in follower`() {
        val synonyms = javaClass.getResourceAsStream("/analyzers/synonyms.txt")
        val config = PathUtils.get(buildDir, leaderClusterPath, "config")
        val synonymPath = config.resolve("synonyms.txt")
        val newSynonymPath = config.resolve("synonyms_new.txt")
        val followerConfig = PathUtils.get(buildDir, followerClusterPath, "config")
        val followerSynonymFilename = "synonyms_follower.txt"
        val followerSynonymPath = followerConfig.resolve(followerSynonymFilename)
        val leaderClient = getClientForCluster(LEADER)
        val followerClient = getClientForCluster(FOLLOWER)
        try {
            Files.copy(synonyms, synonymPath)
            Files.copy(synonyms, followerSynonymPath)

            var settings: Settings = Settings.builder().loadFromStream(synonymsJson, javaClass.getResourceAsStream(synonymsJson), false)
                .build()
            val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName).settings(settings), RequestOptions.DEFAULT)
            assertThat(createIndexResponse.isAcknowledged).isTrue()

            createConnectionBetweenClusters(FOLLOWER, LEADER)
            val overriddenSettings: Settings = Settings.builder()
                .put("index.analysis.filter.my_filter.synonyms_path", followerSynonymFilename)
                .build()
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName, overriddenSettings), waitForRestore = true)
            followerClient.pauseReplication(followerIndexName)
            leaderClient.indices().close(CloseIndexRequest(leaderIndexName), RequestOptions.DEFAULT);

            Files.copy(synonyms, newSynonymPath)
            settings = Settings.builder()
                .put("index.analysis.filter.my_filter.synonyms_path", "synonyms_new.txt")
                .build()
            leaderClient.indices().putSettings(UpdateSettingsRequest(leaderIndexName).settings(settings), RequestOptions.DEFAULT)
            leaderClient.indices().open(OpenIndexRequest(leaderIndexName), RequestOptions.DEFAULT);

            followerClient.resumeReplication(followerIndexName)
            var statusResp = followerClient.replicationStatus(followerIndexName)
            `validate status syncing resposne`(statusResp)
        } finally {
            if (Files.exists(synonymPath)) {
                Files.delete(synonymPath)
            }
            if (Files.exists(followerSynonymPath)) {
                Files.delete(followerSynonymPath)
            }
            if (Files.exists(newSynonymPath)) {
                Files.delete(newSynonymPath)
            }
            try {
                followerClient.stopReplication(followerIndexName)
            } catch (e: Exception) {
                // DO nothing
            }
        }
    }

}
