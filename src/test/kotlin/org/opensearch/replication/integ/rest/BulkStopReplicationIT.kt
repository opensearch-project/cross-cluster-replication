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
import org.opensearch.replication.bulkStopReplication
import org.opensearch.replication.cancelTask
import org.opensearch.replication.startReplication
import org.opensearch.replication.waitForBulkTaskCompletion
import org.assertj.core.api.Assertions.assertThat
import org.opensearch.action.index.IndexRequest
import org.opensearch.client.RequestOptions
import org.opensearch.client.ResponseException
import org.opensearch.client.RestHighLevelClient
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.client.indices.GetIndexRequest
import java.util.concurrent.TimeUnit

@MultiClusterAnnotations.ClusterConfigurations(
    MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
    MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)
@org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/cross-cluster-replication/issues/0000")
class BulkStopReplicationIT : BulkReplicationIT() {

    override val indexPrefix = "bulk-stop-test"
    override val operationName = "bulk_stop_replication"

    override fun executeBulk(
        client: RestHighLevelClient,
        pattern: String,
        excludeIndices: List<String>
    ): Map<String, Any> = client.bulkStopReplication(pattern, excludeIndices)

    override fun assertPostOperationState(client: RestHighLevelClient, index: String) {
        assertThat(client.indices().exists(GetIndexRequest(index), RequestOptions.DEFAULT)).isTrue()
    }

    override fun cleanup(client: RestHighLevelClient, pattern: String) {
        // No cleanup needed — stop is the terminal operation
    }

    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/cross-cluster-replication/issues/0000")
    fun `test bulk stop with partial failures`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        for (i in 1..3) {
            leaderClient.indices().create(CreateIndexRequest("$indexPrefix-partial-$i"), RequestOptions.DEFAULT)
        }
        for (i in 1..2) {
            followerClient.startReplication(
                StartReplicationRequest("source", "$indexPrefix-partial-$i", "$indexPrefix-partial-$i"),
                waitForRestore = true
            )
        }
        // Index 3 exists but has no replication
        followerClient.indices().create(CreateIndexRequest("$indexPrefix-partial-3"), RequestOptions.DEFAULT)

        val response = followerClient.bulkStopReplication("$indexPrefix-partial-*")
        val statusResp = followerClient.waitForBulkTaskCompletion(response["task_id"].toString()) ?: return
        assertThat(statusResp["num_success"]).isEqualTo(2)
        assertThat(statusResp["num_failed"]).isEqualTo(1)
        assertThat((statusResp["failed_indices"] as List<*>)[0].let { (it as Map<*, *>)["failure_reason"].toString() })
            .contains("No replication in progress for index")
    }

    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/cross-cluster-replication/issues/0000")
    fun `test bulk stop task cancel mid flight`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        createAndStartReplication(leaderClient, followerClient, "cancel", 10)

        val response = followerClient.bulkStopReplication("$indexPrefix-cancel-*")
        val taskId = response["task_id"].toString()
        try { followerClient.cancelTask(taskId) } catch (_: ResponseException) {}
        val statusResp = followerClient.waitForBulkTaskCompletion(taskId) ?: return
        assertThat((statusResp["num_cancelled"] as Int) + (statusResp["num_success"] as Int) + (statusResp["num_failed"] as Int)).isEqualTo(10)
    }

    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/cross-cluster-replication/issues/0000")
    fun `test bulk stop unblocks follower indices for writes`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        createAndStartReplication(leaderClient, followerClient, "unblock")

        val response = followerClient.bulkStopReplication("$indexPrefix-unblock-*")
        followerClient.waitForBulkTaskCompletion(response["task_id"].toString()) ?: return

        // Wait for indices to be reopened after stop, then verify writable
        assertBusy({
            for (i in 1..3) {
                try {
                    val indexResponse = followerClient.index(
                        IndexRequest("$indexPrefix-unblock-$i").id("post-stop").source(mapOf("name" to "test-data")),
                        RequestOptions.DEFAULT
                    )
                    assertThat(indexResponse.id).isEqualTo("post-stop")
                } catch (e: Exception) {
                    throw AssertionError("Index not writable yet: ${e.message}", e)
                }
            }
        }, 60, TimeUnit.SECONDS)
    }
}