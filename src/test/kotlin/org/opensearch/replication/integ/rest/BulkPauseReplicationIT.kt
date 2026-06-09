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
import org.opensearch.replication.bulkPauseReplication
import org.opensearch.replication.bulkStopReplication
import org.opensearch.replication.replicationStatus
import org.opensearch.replication.waitForBulkTaskCompletion
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.opensearch.client.ResponseException
import org.opensearch.client.RestHighLevelClient

@MultiClusterAnnotations.ClusterConfigurations(
    MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
    MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)
class BulkPauseReplicationIT : BulkReplicationIT() {

    override val indexPrefix = "bulk-pause-test"
    override val operationName = "bulk_pause_replication"

    override fun executeBulk(
        client: RestHighLevelClient,
        pattern: String,
        excludeIndices: List<String>
    ): Map<String, Any> = client.bulkPauseReplication(pattern, excludeIndices)

    override fun assertPostOperationState(client: RestHighLevelClient, index: String) {
        assertThat(client.replicationStatus(index)["status"]).isEqualTo("PAUSED")
    }

    fun `test bulk pause replication already paused`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        createAndStartReplication(leaderClient, followerClient, "dup")

        followerClient.bulkPauseReplication("$indexPrefix-dup-*")
            .also { followerClient.waitForBulkTaskCompletion(it["task_id"].toString()) }

        assertThatThrownBy {
            followerClient.bulkPauseReplication("$indexPrefix-dup-*")
        }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining("400")
            .hasMessageContaining("is already paused")

        followerClient.bulkStopReplication("$indexPrefix-dup-*")
    }

    fun `test bulk pause with partial failures`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        createAndStartReplication(leaderClient, followerClient, "partial", 4)

        // Pause indices 3 and 4 first
        followerClient.bulkPauseReplication(
            pattern = "$indexPrefix-partial-*",
            excludeIndices = listOf("$indexPrefix-partial-1", "$indexPrefix-partial-2")
        ).also { followerClient.waitForBulkTaskCompletion(it["task_id"].toString()) }

        // Now pause all — 1,2 succeed, 3,4 fail (already paused)
        val response = followerClient.bulkPauseReplication("$indexPrefix-partial-*")
        val statusResp = followerClient.waitForBulkTaskCompletion(response["task_id"].toString()) ?: return
        assertThat(statusResp["num_success"]).isEqualTo(2)
        assertThat(statusResp["num_failed"]).isEqualTo(2)

        followerClient.bulkStopReplication("$indexPrefix-partial-*")
    }
}
