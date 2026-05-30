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
import org.opensearch.replication.forceResumeReplication
import org.opensearch.replication.pauseReplication
import org.opensearch.replication.replicationStatus
import org.opensearch.replication.resumeReplication
import org.opensearch.replication.startReplication
import org.opensearch.replication.stopReplication
import org.opensearch.replication.`validate paused status response`
import org.opensearch.replication.`validate status syncing response`
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.opensearch.action.DocWriteResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.client.RequestOptions
import org.opensearch.client.ResponseException
import org.opensearch.client.indices.CreateIndexRequest
import org.junit.Assert
import java.util.concurrent.TimeUnit

@MultiClusterAnnotations.ClusterConfigurations(
    MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
    MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)
class ForceResumeReplicationIT : MultiClusterRestTestCase() {
    private val leaderIndexName = "leader_index"
    private val followerIndexName = "force_resumed_index"

    fun `test force resume after retention lease expires`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        // Create leader index and start replication
        val createIndexResponse = leaderClient.indices().create(
            CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT
        )
        assertThat(createIndexResponse.isAcknowledged).isTrue()

        followerClient.startReplication(
            StartReplicationRequest("source", leaderIndexName, followerIndexName),
            waitForRestore = true
        )

        // Index some data on leader to advance the global checkpoint
        val sourceMap: MutableMap<String, String> = HashMap()
        sourceMap["field1"] = "value1"
        val indexResponse = leaderClient.index(
            IndexRequest(leaderIndexName).id("1").source(sourceMap), RequestOptions.DEFAULT
        )
        assertThat(indexResponse.result).isIn(DocWriteResponse.Result.CREATED, DocWriteResponse.Result.UPDATED)

        // Wait for replication to sync
        assertBusy({
            val statusResp = followerClient.replicationStatus(followerIndexName)
            `validate status syncing response`(statusResp)
        }, 30, TimeUnit.SECONDS)

        // Pause replication
        followerClient.pauseReplication(followerIndexName)
        val statusResp = followerClient.replicationStatus(followerIndexName)
        `validate paused status response`(statusResp)

        // Simulate retention lease expiry by setting the lease period to 1s on the leader,
        // waiting for the lease to be garbage-collected, then restoring the default period.
        val expireSettingsRequest = org.opensearch.client.Request("PUT", "/$leaderIndexName/_settings")
        expireSettingsRequest.setJsonEntity("""{"index.soft_deletes.retention_lease.period": "1s"}""")
        leaderClient.lowLevelClient.performRequest(expireSettingsRequest)

        // Wait for the lease to expire and be garbage-collected
        Thread.sleep(30000)

        // Restore the retention lease period to default
        val restoreSettingsRequest = org.opensearch.client.Request("PUT", "/$leaderIndexName/_settings")
        restoreSettingsRequest.setJsonEntity("""{"index.soft_deletes.retention_lease.period": "12h"}""")
        leaderClient.lowLevelClient.performRequest(restoreSettingsRequest)

        // Normal resume should fail because retention leases are gone
        assertThatThrownBy {
            followerClient.resumeReplication(followerIndexName)
        }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining("Retention lease doesn't exist")

        // Force resume should succeed — triggers snapshot bootstrap
        followerClient.forceResumeReplication(followerIndexName)

        // After force resume, replication goes through BOOTSTRAPPING (snapshot restore)
        // before reaching SYNCING. Verify replication is active (not PAUSED/failed).
        assertBusy({
            val syncStatus = followerClient.replicationStatus(followerIndexName)
            val status = syncStatus.getValue("status") as String
            Assert.assertTrue(
                "Expected SYNCING or BOOTSTRAPPING but was $status",
                status == "SYNCING" || status == "BOOTSTRAPPING"
            )
        }, 60, TimeUnit.SECONDS)

        // Cleanup
        followerClient.stopReplication(followerIndexName)
    }

    fun `test force resume when retention leases still exist proceeds normally`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        // Create leader index and start replication
        val createIndexResponse = leaderClient.indices().create(
            CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT
        )
        assertThat(createIndexResponse.isAcknowledged).isTrue()

        followerClient.startReplication(
            StartReplicationRequest("source", leaderIndexName, followerIndexName),
            waitForRestore = true
        )

        // Wait for syncing state
        assertBusy({
            val statusResp = followerClient.replicationStatus(followerIndexName)
            `validate status syncing response`(statusResp)
        }, 30, TimeUnit.SECONDS)

        // Pause replication (retention leases are still valid)
        followerClient.pauseReplication(followerIndexName)

        // Force resume with valid leases should succeed (falls through to normal resume)
        followerClient.forceResumeReplication(followerIndexName)

        // Verify replication is back in syncing state
        assertBusy({
            val syncStatus = followerClient.replicationStatus(followerIndexName)
            `validate status syncing response`(syncStatus)
        }, 30, TimeUnit.SECONDS)

        // Cleanup
        followerClient.stopReplication(followerIndexName)
    }

    fun `test force resume error message suggests force_resume option`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        // Create leader index and start replication
        val createIndexResponse = leaderClient.indices().create(
            CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT
        )
        assertThat(createIndexResponse.isAcknowledged).isTrue()

        followerClient.startReplication(
            StartReplicationRequest("source", leaderIndexName, followerIndexName),
            waitForRestore = true
        )

        // Pause and expire retention leases
        followerClient.pauseReplication(followerIndexName)
        val expireSettingsRequest = org.opensearch.client.Request("PUT", "/$leaderIndexName/_settings")
        expireSettingsRequest.setJsonEntity("""{"index.soft_deletes.retention_lease.period": "1s"}""")
        leaderClient.lowLevelClient.performRequest(expireSettingsRequest)
        Thread.sleep(30000)
        val restoreSettingsRequest = org.opensearch.client.Request("PUT", "/$leaderIndexName/_settings")
        restoreSettingsRequest.setJsonEntity("""{"index.soft_deletes.retention_lease.period": "12h"}""")
        leaderClient.lowLevelClient.performRequest(restoreSettingsRequest)

        // Normal resume should fail with a helpful error message
        assertThatThrownBy {
            followerClient.resumeReplication(followerIndexName)
        }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining("force_resume=true")
    }
}
