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

import org.assertj.core.api.Assertions
import org.junit.Assert
import org.opensearch.client.RequestOptions
import org.opensearch.client.ResponseException
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.replication.MultiClusterAnnotations
import org.opensearch.replication.MultiClusterRestTestCase
import org.opensearch.replication.startReplication
import org.opensearch.replication.stopReplication
import org.opensearch.replication.StartReplicationRequest
import org.opensearch.replication.replicationStatus
import org.opensearch.replication.`validate status syncing response`
import java.util.concurrent.TimeUnit

@MultiClusterAnnotations.ClusterConfigurations(
    MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
    MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)
class ReplicationStatusIT: MultiClusterRestTestCase() {

    fun `test replication status with valid params`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val indexName = "test-status-valid-param"
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(indexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            followerClient.startReplication(StartReplicationRequest("source", indexName, indexName), waitForRestore = true)
            assertBusy({
                var statusResp = followerClient.replicationStatus(indexName)
                `validate status syncing response`(statusResp)
            }, 30, TimeUnit.SECONDS)
        } finally {
            followerClient.stopReplication(indexName)
        }
    }

    fun `test replication status without valid params`() {
        val followerClient = getClientForCluster(FOLLOWER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        try {
            followerClient.replicationStatus("")
            Assert.fail("Status API shouldn't succeed in this case")
        } catch (e: ResponseException) {
            Assert.assertEquals(e.response.statusLine.statusCode, 400)
            Assert.assertTrue(e.message != null)
            Assert.assertTrue(e.message!!.contains("Index name must be specified to obtain replication status"))
        }
    }
}
