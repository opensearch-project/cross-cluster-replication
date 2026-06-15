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
import org.opensearch.replication.StartReplicationRequest
import org.opensearch.replication.bulkPauseReplication
import org.opensearch.replication.bulkResumeReplication
import org.opensearch.replication.bulkStopReplication
import org.opensearch.replication.cancelTask
import org.opensearch.replication.replicationStatus
import org.opensearch.replication.startReplication
import org.opensearch.replication.waitForBulkTaskCompletion
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.ResponseException
import org.opensearch.client.RestHighLevelClient
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.common.unit.TimeValue

@MultiClusterAnnotations.ClusterConfigurations(
    MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
    MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)
class BulkResumeReplicationIT : BulkReplicationIT() {

    override val indexPrefix = "bulk-resume-test"
    override val operationName = "bulk_resume_replication"

    override fun executeBulk(
        client: RestHighLevelClient,
        pattern: String,
        excludeIndices: List<String>
    ): Map<String, Any> = client.bulkResumeReplication(pattern, excludeIndices)

    override fun assertPostOperationState(client: RestHighLevelClient, index: String) {
        assertThat(client.replicationStatus(index)["status"]).isEqualTo("SYNCING")
    }

    override fun setupPreCondition(client: RestHighLevelClient, pattern: String) {
        client.bulkPauseReplication(pattern)
            .also { client.waitForBulkTaskCompletion(it["task_id"].toString()) }
    }

    fun `test bulk resume without pause`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        createAndStartReplication(leaderClient, followerClient, "running")

        assertThatThrownBy {
            followerClient.bulkResumeReplication("$indexPrefix-running-*")
        }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining("400")
            .hasMessageContaining("is already running")

        followerClient.bulkStopReplication("$indexPrefix-running-*")
    }

    fun `test bulk resume with partial failures`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        createAndStartReplication(leaderClient, followerClient, "partial", 4)

        // Pause only indices 1 and 2
        followerClient.bulkPauseReplication(
            pattern = "$indexPrefix-partial-*",
            excludeIndices = listOf("$indexPrefix-partial-3", "$indexPrefix-partial-4")
        ).also { followerClient.waitForBulkTaskCompletion(it["task_id"].toString()) }

        // Resume all — 1,2 succeed (PAUSED), 3,4 fail (SYNCING)
        val response = followerClient.bulkResumeReplication("$indexPrefix-partial-*")
        val statusResp = followerClient.waitForBulkTaskCompletion(response["task_id"].toString()) ?: return
        assertThat(statusResp["num_success"]).isEqualTo(2)
        assertThat(statusResp["num_failed"]).isEqualTo(2)
        (statusResp["failed_indices"] as List<*>).forEach { fi ->
            assertThat((fi as Map<*, *>)["failure_reason"].toString()).contains("is already running")
        }

        followerClient.bulkStopReplication("$indexPrefix-partial-*")
    }

    fun `test bulk resume task cancel mid flight`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        createAndStartReplication(leaderClient, followerClient, "cancel", 10)

        followerClient.bulkPauseReplication("$indexPrefix-cancel-*")
            .also { followerClient.waitForBulkTaskCompletion(it["task_id"].toString()) }

        setBulkBatchSize(followerClient, 1)

        val response = followerClient.bulkResumeReplication("$indexPrefix-cancel-*")
        val taskId = response["task_id"].toString()
        try { followerClient.cancelTask(taskId) } catch (_: ResponseException) {}
        val statusResp = followerClient.waitForBulkTaskCompletion(taskId, TimeValue.timeValueSeconds(180)) ?: return
        assertThat((statusResp["num_cancelled"] as Int) + (statusResp["num_success"] as Int) + (statusResp["num_failed"] as Int)).isEqualTo(10)

        setBulkBatchSize(followerClient, 10)
        followerClient.bulkStopReplication("$indexPrefix-cancel-*")
    }

    fun `test bulk resume with lost retention lease on some indices`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        createAndStartReplication(leaderClient, followerClient, "lease", 3)

        followerClient.bulkPauseReplication("$indexPrefix-lease-*")
            .also { followerClient.waitForBulkTaskCompletion(it["task_id"].toString()) }

        // Delete and recreate leader index 1 to lose its retention lease
        leaderClient.indices().delete(DeleteIndexRequest("$indexPrefix-lease-1"), RequestOptions.DEFAULT)
        leaderClient.indices().create(CreateIndexRequest("$indexPrefix-lease-1"), RequestOptions.DEFAULT)

        // Resume all — index 1 should fail (no retention lease), 2 and 3 should succeed
        val response = followerClient.bulkResumeReplication("$indexPrefix-lease-*")
        val statusResp = followerClient.waitForBulkTaskCompletion(response["task_id"].toString()) ?: return
        assertThat(statusResp["num_success"]).isEqualTo(2)
        assertThat(statusResp["num_failed"]).isEqualTo(1)
        (statusResp["failed_indices"] as List<*>).forEach { fi ->
            assertThat((fi as Map<*, *>)["failure_reason"].toString()).contains("Retention lease")
        }

        followerClient.bulkStopReplication("$indexPrefix-lease-*")
    }

    private fun setBulkBatchSize(client: RestHighLevelClient, size: Int) {
        val request = Request("PUT", "/_cluster/settings")
        request.setJsonEntity("""{"persistent": {"plugins.replication.follower.bulk_batch_size": $size}}""")
        client.lowLevelClient.performRequest(request)
    }
}
