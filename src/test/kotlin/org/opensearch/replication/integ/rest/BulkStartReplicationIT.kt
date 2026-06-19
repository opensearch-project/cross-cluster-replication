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
import org.opensearch.replication.`validate bulk response`
import org.opensearch.replication.`validate task status response`
import org.opensearch.replication.bulkStartReplication
import org.opensearch.replication.bulkStatus
import org.opensearch.replication.bulkStopReplication
import org.opensearch.replication.cancelTask
import org.opensearch.replication.getTaskStatus
import org.opensearch.replication.replicationStatus
import org.opensearch.replication.waitForBulkTaskCompletion
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.ResponseException
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.common.unit.TimeValue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

@MultiClusterAnnotations.ClusterConfigurations(
    MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
    MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)
class BulkStartReplicationIT : MultiClusterRestTestCase() {

    private val indexPrefix = "bulk-start-test"

    @org.junit.Before
    fun setupBatchSize() {
        val followerClient = getClientForCluster(FOLLOWER)
        setBulkBatchSize(followerClient, 1)
    }

    @org.junit.After
    fun resetBatchSize() {
        val followerClient = getClientForCluster(FOLLOWER)
        setBulkBatchSize(followerClient, 10)
    }

    fun `test bulk start replication in following state`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        for (i in 1..3) {
            leaderClient.indices().create(CreateIndexRequest("$indexPrefix-$i"), RequestOptions.DEFAULT)
        }

        val response = followerClient.bulkStartReplication(pattern = "$indexPrefix-*", leaderAlias = "source")
        `validate bulk response`(response)
        val statusResp = followerClient.waitForBulkTaskCompletion(response["task_id"].toString()) ?: return
        `validate task status response`(statusResp, "bulk_start_replication", "$indexPrefix-*")
        assertThat(statusResp["num_success"]).isEqualTo(3)

        assertBusy({
            for (i in 1..3) {
                assertThat(followerClient.replicationStatus("$indexPrefix-$i")["status"])
                    .isIn("SYNCING", "BOOTSTRAPPING")
            }
        }, 180, TimeUnit.SECONDS)

        followerClient.bulkStopReplication("$indexPrefix-*")
    }

    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/cross-cluster-replication/issues/0000")
    fun `test bulk start with pattern not found on leader`() {
        val followerClient = getClientForCluster(FOLLOWER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        assertThatThrownBy {
            followerClient.bulkStartReplication(pattern = "non-existent-*", leaderAlias = "source")
        }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining("404")
            .hasMessageContaining("No indices found matching pattern")
    }

    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/cross-cluster-replication/issues/0000")
    fun `test bulk start with all indices already replicated`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        for (i in 1..3) {
            leaderClient.indices().create(CreateIndexRequest("$indexPrefix-dup-$i"), RequestOptions.DEFAULT)
        }

        followerClient.bulkStartReplication(pattern = "$indexPrefix-dup-*", leaderAlias = "source")
            .also { followerClient.waitForBulkTaskCompletion(it["task_id"].toString()) }

        assertThatThrownBy {
            followerClient.bulkStartReplication(pattern = "$indexPrefix-dup-*", leaderAlias = "source")
        }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining("400")
            .hasMessageContaining("Can't use same index again for replication")

        followerClient.bulkStopReplication("$indexPrefix-dup-*")
    }

    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/cross-cluster-replication/issues/0000")
    fun `test bulk start without leader alias`() {
        val followerClient = getClientForCluster(FOLLOWER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        assertThatThrownBy {
            followerClient.bulkStartReplication(pattern = "$indexPrefix-*")
        }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining("400")
            .hasMessageContaining("leader_alias is required for bulk start")
    }

    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/cross-cluster-replication/issues/0000")
    fun `test bulk start with partial failures`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        for (i in 1..3) {
            leaderClient.indices().create(CreateIndexRequest("$indexPrefix-partial-$i"), RequestOptions.DEFAULT)
        }

        followerClient.bulkStartReplication(pattern = "$indexPrefix-partial-1", leaderAlias = "source")
            .also { followerClient.waitForBulkTaskCompletion(it["task_id"].toString()) }

        val response = followerClient.bulkStartReplication(pattern = "$indexPrefix-partial-*", leaderAlias = "source")
        `validate bulk response`(response)
        val statusResp = followerClient.waitForBulkTaskCompletion(response["task_id"].toString()) ?: return
        assertThat(statusResp["num_success"]).isEqualTo(2)
        assertThat(statusResp["num_failed"]).isEqualTo(1)

        followerClient.bulkStopReplication("$indexPrefix-partial-*")
    }

    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/cross-cluster-replication/issues/0000")
    fun `test bulk start with exclude index filter`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        for (i in 1..3) {
            leaderClient.indices().create(CreateIndexRequest("$indexPrefix-excl-$i"), RequestOptions.DEFAULT)
        }

        val response = followerClient.bulkStartReplication(
            pattern = "$indexPrefix-excl-*",
            leaderAlias = "source",
            excludeIndices = listOf("$indexPrefix-excl-3")
        )
        `validate bulk response`(response)
        val _s = followerClient.waitForBulkTaskCompletion(response["task_id"].toString()) ?: return
        assertThat(_s["num_success"]).isEqualTo(2)
        assertThat((followerClient.bulkStatus("$indexPrefix-excl-*")["indices"] as Map<*, *>).keys)
            .doesNotContain("$indexPrefix-excl-3")

        followerClient.bulkStopReplication("$indexPrefix-excl-*")
    }

    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/cross-cluster-replication/issues/0000")
    fun `test bulk start only one task at a time`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        for (i in 1..5) {
            leaderClient.indices().create(CreateIndexRequest("$indexPrefix-concurrent-$i"), RequestOptions.DEFAULT)
        }

        setBulkBatchSize(followerClient, 1)

        // Fire two requests concurrently — one must succeed, the other must be rejected.
        // Using a latch ensures both threads send their HTTP request at nearly the same instant.
        val startLatch = CountDownLatch(1)
        val result1 = AtomicReference<Any>()
        val result2 = AtomicReference<Any>()

        val t1 = Thread {
            startLatch.await()
            try {
                result1.set(followerClient.bulkStartReplication(pattern = "$indexPrefix-concurrent-*", leaderAlias = "source"))
            } catch (e: ResponseException) { result1.set(e) }
        }
        val t2 = Thread {
            startLatch.await()
            try {
                result2.set(followerClient.bulkStartReplication(pattern = "$indexPrefix-concurrent-*", leaderAlias = "source"))
            } catch (e: ResponseException) { result2.set(e) }
        }

        t1.start()
        t2.start()
        startLatch.countDown()
        t1.join(60_000)
        t2.join(60_000)

        val results = listOf(result1.get(), result2.get())
        val successes = results.filterIsInstance<Map<*, *>>()
        val failures = results.filterIsInstance<ResponseException>()

        assertThat(successes).withFailMessage("Expected exactly one success but got: $results").hasSize(1)
        assertThat(failures).withFailMessage("Expected exactly one rejection but got: $results").hasSize(1)
        assertThat(failures[0].message).contains("A bulk replication task is already running")

        val taskId = successes[0]["task_id"].toString()
        try { followerClient.cancelTask(taskId) } catch (_: ResponseException) {}
        followerClient.waitForBulkTaskCompletion(taskId, TimeValue.timeValueSeconds(180))
        setBulkBatchSize(followerClient, 10)
    }

    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/cross-cluster-replication/issues/0000")
    fun `test bulk start task cancel mid flight`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        for (i in 1..10) {
            leaderClient.indices().create(CreateIndexRequest("$indexPrefix-cancel-$i"), RequestOptions.DEFAULT)
        }

        setBulkBatchSize(followerClient, 1)

        val response = followerClient.bulkStartReplication(pattern = "$indexPrefix-cancel-*", leaderAlias = "source")
        val taskId = response["task_id"].toString()
        try {
            followerClient.cancelTask(taskId)
        } catch (e: ResponseException) {
            // Task may complete before cancel arrives — acceptable
            assertThat(e.response.statusLine.statusCode).isEqualTo(404)
        }

        val statusResp = followerClient.waitForBulkTaskCompletion(taskId, TimeValue.timeValueSeconds(180)) ?: return
        val cancelled = statusResp["num_cancelled"] as Int
        val success = statusResp["num_success"] as Int
        val failed = statusResp["num_failed"] as Int
        assertThat(cancelled + success + failed).isEqualTo(10)

        setBulkBatchSize(followerClient, 10)
    }

    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/cross-cluster-replication/issues/0000")
    fun `test bulk start task status response`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        for (i in 1..3) {
            leaderClient.indices().create(CreateIndexRequest("$indexPrefix-status-$i"), RequestOptions.DEFAULT)
        }

        val response = followerClient.bulkStartReplication(pattern = "$indexPrefix-status-*", leaderAlias = "source")
        val taskId = response["task_id"].toString()
        val statusResp = followerClient.waitForBulkTaskCompletion(taskId) ?: return
        assertThat(statusResp["operation_type"]).isEqualTo("bulk_start_replication")
        assertThat(statusResp["num_success"]).isEqualTo(3)
        assertThat(statusResp["num_failed"]).isEqualTo(0)
        assertThat(statusResp["num_pending"]).isEqualTo(0)
        assertThat(statusResp["num_cancelled"]).isEqualTo(0)
        assertThat(statusResp["failed_indices"] as List<*>).isEmpty()
        assertThat(statusResp).containsKey("start_time")

        followerClient.bulkStopReplication("$indexPrefix-status-*")
    }

    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/cross-cluster-replication/issues/0000")
    fun `test bulk start task status unknown task id`() {
        val followerClient = getClientForCluster(FOLLOWER)
        assertThatThrownBy {
            followerClient.getTaskStatus("unknownNode:99999")
        }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining("404")
    }

    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/cross-cluster-replication/issues/0000")
    fun `test bulk start task cancel with completed task`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        leaderClient.indices().create(CreateIndexRequest("$indexPrefix-done"), RequestOptions.DEFAULT)
        val response = followerClient.bulkStartReplication(pattern = "$indexPrefix-done", leaderAlias = "source")
        val taskId = response["task_id"].toString()
        followerClient.waitForBulkTaskCompletion(taskId)

        // After task completion, cancel may return 404 (task already unregistered)
        // or 200 (task still in TaskManager but effectively done). Both are acceptable.
        try {
            val cancelResp = followerClient.cancelTask(taskId)
            assertThat(cancelResp["acknowledged"]).isEqualTo(true)
        } catch (e: ResponseException) {
            assertThat(e.response.statusLine.statusCode).isEqualTo(404)
        }

        followerClient.bulkStopReplication("$indexPrefix-done")
    }

    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/cross-cluster-replication/issues/0000")
    fun `test bulk start fails when leader unreachable`() {
        val followerClient = getClientForCluster(FOLLOWER)

        assertThatThrownBy {
            followerClient.bulkStartReplication(
                pattern = "$indexPrefix-cluster-*",
                leaderAlias = "non-existent-cluster"
            )
        }.isInstanceOf(ResponseException::class.java)
    }

    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/cross-cluster-replication/issues/0000")
    fun `test bulk batch size setting controls batching`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        for (i in 1..6) {
            leaderClient.indices().create(CreateIndexRequest("$indexPrefix-batch-$i"), RequestOptions.DEFAULT)
        }

        setBulkBatchSize(followerClient, 2)

        val response = followerClient.bulkStartReplication(pattern = "$indexPrefix-batch-*", leaderAlias = "source")
        val _s = followerClient.waitForBulkTaskCompletion(response["task_id"].toString()) ?: return
        assertThat(_s["num_success"]).isEqualTo(6)

        setBulkBatchSize(followerClient, 10)
        followerClient.bulkStopReplication("$indexPrefix-batch-*")
    }

    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/cross-cluster-replication/issues/0000")
    fun `test bulk status shows syncing for replicating indices`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        for (i in 1..3) {
            leaderClient.indices().create(CreateIndexRequest("$indexPrefix-bstatus-$i"), RequestOptions.DEFAULT)
        }

        followerClient.bulkStartReplication(pattern = "$indexPrefix-bstatus-*", leaderAlias = "source")
            .also { followerClient.waitForBulkTaskCompletion(it["task_id"].toString()) }

        assertBusy({
            val statusResp = followerClient.bulkStatus("$indexPrefix-bstatus-*")
            val indices = statusResp["indices"] as Map<*, *>
            assertThat(indices).hasSize(3)
            indices.values.forEach { idx ->
                val indexStatus = idx as Map<*, *>
                assertThat(indexStatus["status"]).isIn("SYNCING", "BOOTSTRAPPING")
            }
        }, 180, TimeUnit.SECONDS)

        followerClient.bulkStopReplication("$indexPrefix-bstatus-*")
    }

    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/cross-cluster-replication/issues/0000")
    fun `test bulk status returns empty when pattern not found`() {
        val followerClient = getClientForCluster(FOLLOWER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        assertThat(followerClient.bulkStatus("non-existent-bulk-status-*")["indices"] as Map<*, *>).isEmpty()
    }

    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/cross-cluster-replication/issues/0000")
    fun `test bulk status without pattern param`() {
        val followerClient = getClientForCluster(FOLLOWER)

        assertThatThrownBy {
            followerClient.lowLevelClient.performRequest(Request("GET", "/_plugins/_replication/_bulk_status"))
        }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining("400")
            .hasMessageContaining("pattern parameter is required")
    }

    private fun setBulkBatchSize(client: org.opensearch.client.RestHighLevelClient, size: Int) {
        val request = Request("PUT", "/_cluster/settings")
        request.setJsonEntity("""{"persistent": {"plugins.replication.follower.bulk_batch_size": $size}}""")
        client.lowLevelClient.performRequest(request)
    }
}
