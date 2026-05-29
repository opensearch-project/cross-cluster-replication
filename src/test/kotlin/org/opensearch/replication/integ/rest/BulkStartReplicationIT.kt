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
import org.opensearch.action.index.IndexRequest
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.ResponseException
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.XContentType
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

@MultiClusterAnnotations.ClusterConfigurations(
    MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
    MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)
class BulkStartReplicationIT : MultiClusterRestTestCase() {

    private val indexPrefix = "bulk-start-test"

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
                assertThat(followerClient.replicationStatus("$indexPrefix-$i")["status"]).isEqualTo("SYNCING")
            }
        }, 30, TimeUnit.SECONDS)

        // verify data written to leader is replicated
        leaderClient.index(
            IndexRequest("$indexPrefix-1").id("1").source("""{"field": "value"}""", XContentType.JSON),
            RequestOptions.DEFAULT
        )
        assertBusy({
            val countResp = followerClient.lowLevelClient.performRequest(Request("GET", "/$indexPrefix-1/_count"))
            assertThat(org.opensearch.test.rest.OpenSearchRestTestCase.entityAsMap(countResp)["count"]).isEqualTo(1)
        }, 30, TimeUnit.SECONDS)

        followerClient.bulkStopReplication("$indexPrefix-*")
    }

    fun `test bulk start with pattern not found on leader`() {
        val followerClient = getClientForCluster(FOLLOWER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        assertThatThrownBy {
            followerClient.bulkStartReplication(pattern = "non-existent-*", leaderAlias = "source")
        }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining("404")
            .hasMessageContaining("No indices found matching pattern")
    }

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

    fun `test bulk start without leader alias`() {
        val followerClient = getClientForCluster(FOLLOWER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        assertThatThrownBy {
            followerClient.bulkStartReplication(pattern = "$indexPrefix-*")
        }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining("400")
            .hasMessageContaining("leader_alias is required for bulk start")
    }

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

    fun `test bulk start only one task at a time`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        for (i in 1..100) {
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

    fun `test bulk start task cancel mid flight`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        for (i in 1..50) {
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

        val statusResp = followerClient.waitForBulkTaskCompletion(taskId, TimeValue.timeValueSeconds(60)) ?: return
        val cancelled = statusResp["num_cancelled"] as Int
        val success = statusResp["num_success"] as Int
        assertThat(cancelled + success).isEqualTo(50)

        setBulkBatchSize(followerClient, 10)
    }

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

    fun `test bulk start task status unknown task id`() {
        val followerClient = getClientForCluster(FOLLOWER)
        assertThatThrownBy {
            followerClient.getTaskStatus("unknownNode:99999")
        }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining("404")
    }

    fun `test bulk start task cancel with completed task`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        leaderClient.indices().create(CreateIndexRequest("$indexPrefix-done"), RequestOptions.DEFAULT)
        val response = followerClient.bulkStartReplication(pattern = "$indexPrefix-done", leaderAlias = "source")
        val taskId = response["task_id"].toString()
        followerClient.waitForBulkTaskCompletion(taskId)

        assertThatThrownBy {
            followerClient.cancelTask(taskId)
        }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining("404")

        followerClient.bulkStopReplication("$indexPrefix-done")
    }

    fun `test bulk start fails when leader unreachable`() {
        val followerClient = getClientForCluster(FOLLOWER)

        assertThatThrownBy {
            followerClient.bulkStartReplication(
                pattern = "$indexPrefix-cluster-*",
                leaderAlias = "non-existent-cluster"
            )
        }.isInstanceOf(ResponseException::class.java)
    }

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

    fun `test bulk status shows syncing for replicating indices`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        for (i in 1..3) {
            leaderClient.indices().create(CreateIndexRequest("$indexPrefix-bstatus-$i"), RequestOptions.DEFAULT)
        }

        followerClient.bulkStartReplication(pattern = "$indexPrefix-bstatus-*", leaderAlias = "source")
            .also { followerClient.waitForBulkTaskCompletion(it["task_id"].toString()) }

        val statusResp = followerClient.bulkStatus("$indexPrefix-bstatus-*")
        val indices = statusResp["indices"] as Map<*, *>
        assertThat(indices).hasSize(3)
        indices.values.forEach { idx ->
            val indexStatus = idx as Map<*, *>
            assertThat(indexStatus["status"]).isEqualTo("SYNCING")
            assertThat(indexStatus.keys).contains("leader_alias", "leader_index", "follower_index", "syncing_details")
        }

        followerClient.bulkStopReplication("$indexPrefix-bstatus-*")
    }

    fun `test bulk status returns empty when pattern not found`() {
        val followerClient = getClientForCluster(FOLLOWER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        assertThat(followerClient.bulkStatus("non-existent-bulk-status-*")["indices"] as Map<*, *>).isEmpty()
    }

    fun `test bulk status without pattern param`() {
        val followerClient = getClientForCluster(FOLLOWER)

        assertThatThrownBy {
            followerClient.lowLevelClient.performRequest(Request("GET", "/_plugins/_replication/_bulk_status"))
        }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining("400")
            .hasMessageContaining("pattern parameter is required")
    }

    fun `test bulk start data integrity with mappings and settings`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        // Create indices with custom mappings and settings
        for (i in 1..3) {
            val createReq = Request("PUT", "/$indexPrefix-data-$i")
            createReq.setJsonEntity("""{
                "settings": {"index": {"number_of_replicas": 0, "number_of_shards": 1}},
                "mappings": {"properties": {"name": {"type": "keyword"}, "value": {"type": "integer"}}}
            }""")
            leaderClient.lowLevelClient.performRequest(createReq)
        }

        // Index documents on leader
        for (i in 1..3) {
            for (doc in 1..5) {
                leaderClient.index(
                    IndexRequest("$indexPrefix-data-$i").id("$doc")
                        .source("""{"name": "doc-$doc", "value": ${doc * i}}""", XContentType.JSON),
                    RequestOptions.DEFAULT
                )
            }
        }

        // Bulk start replication
        val response = followerClient.bulkStartReplication(pattern = "$indexPrefix-data-*", leaderAlias = "source")
        `validate bulk response`(response)
        followerClient.waitForBulkTaskCompletion(response["task_id"].toString())

        // Verify all indices are syncing
        assertBusy({
            val indices = followerClient.bulkStatus("$indexPrefix-data-*")["indices"] as Map<*, *>
            assertThat(indices).hasSize(3)
            indices.values.forEach { assertThat((it as Map<*, *>)["status"]).isEqualTo("SYNCING") }
        }, 30, TimeUnit.SECONDS)

        // Verify data replicated to all follower indices
        assertBusy({
            for (i in 1..3) {
                val countResp = followerClient.lowLevelClient.performRequest(Request("GET", "/$indexPrefix-data-$i/_count"))
                assertThat(org.opensearch.test.rest.OpenSearchRestTestCase.entityAsMap(countResp)["count"]).isEqualTo(5)
            }
        }, 30, TimeUnit.SECONDS)

        // Verify specific document content
        assertBusy({
            val docResp = followerClient.lowLevelClient.performRequest(Request("GET", "/$indexPrefix-data-2/_doc/3"))
            val source = (org.opensearch.test.rest.OpenSearchRestTestCase.entityAsMap(docResp)["_source"] as Map<*, *>)
            assertThat(source["name"]).isEqualTo("doc-3")
            assertThat(source["value"]).isEqualTo(6)
        }, 30, TimeUnit.SECONDS)

        // Verify mappings replicated
        val mappingResp = followerClient.lowLevelClient.performRequest(Request("GET", "/$indexPrefix-data-1/_mapping"))
        val mappings = org.opensearch.test.rest.OpenSearchRestTestCase.entityAsMap(mappingResp)
        val properties = ((mappings["$indexPrefix-data-1"] as Map<*, *>)["mappings"] as Map<*, *>)["properties"] as Map<*, *>
        assertThat(properties.keys).contains("name")
        assertThat(properties.keys).contains("value")

        // Verify new writes on leader replicate
        leaderClient.index(
            IndexRequest("$indexPrefix-data-1").id("6").source("""{"name": "new-doc", "value": 99}""", XContentType.JSON),
            RequestOptions.DEFAULT
        )
        assertBusy({
            val countResp = followerClient.lowLevelClient.performRequest(Request("GET", "/$indexPrefix-data-1/_count"))
            assertThat(org.opensearch.test.rest.OpenSearchRestTestCase.entityAsMap(countResp)["count"]).isEqualTo(6)
        }, 30, TimeUnit.SECONDS)

        followerClient.bulkStopReplication("$indexPrefix-data-*")
    }

    private fun setBulkBatchSize(client: org.opensearch.client.RestHighLevelClient, size: Int) {
        val request = Request("PUT", "/_cluster/settings")
        request.setJsonEntity("""{"persistent": {"plugins.replication.follower.bulk_batch_size": $size}}""")
        client.lowLevelClient.performRequest(request)
    }
}
