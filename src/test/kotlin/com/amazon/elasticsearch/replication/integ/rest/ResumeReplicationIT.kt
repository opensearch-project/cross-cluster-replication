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
import org.apache.http.util.EntityUtils
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.DocWriteResponse
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.flush.FlushRequest
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Request
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.ResponseException
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.indices.CloseIndexRequest
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.mapper.MapperService
import org.elasticsearch.test.ESTestCase.assertBusy
import org.junit.Assert
import java.util.concurrent.TimeUnit



@MultiClusterAnnotations.ClusterConfigurations(
        MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
        MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)
class ResumeReplicationIT: MultiClusterRestTestCase() {
    private val leaderIndexName = "leader_index"
    private val followerIndexName = "resumed_index"

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
            Assert.assertEquals(statusResp.getValue("status"),"PAUSED")
            followerClient.resumeReplication(followerIndexName)
            statusResp = followerClient.replicationStatus(followerIndexName)
            Assert.assertNotEquals(statusResp.getValue("status"),"PAUSED")
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
                followerClient.resumeReplication(followerIndexName)
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

            var statusResp = followerClient.replicationStatus(followerIndexName)
            Assert.assertEquals(statusResp.getValue("status"),"PAUSED")
            followerClient.resumeReplication(followerIndexName)
            statusResp = followerClient.replicationStatus(followerIndexName)
            Assert.assertNotEquals(statusResp.getValue("status"),"PAUSED")
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


}
