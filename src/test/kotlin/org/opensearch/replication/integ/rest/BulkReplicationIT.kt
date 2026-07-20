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

import org.opensearch.replication.MultiClusterRestTestCase
import org.opensearch.replication.StartReplicationRequest
import org.opensearch.replication.`validate bulk response`
import org.opensearch.replication.`validate task status response`
import org.opensearch.replication.bulkStopReplication
import org.opensearch.replication.startReplication
import org.opensearch.replication.waitForBulkTaskCompletion
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.opensearch.client.RequestOptions
import org.opensearch.client.ResponseException
import org.opensearch.client.RestHighLevelClient
import org.opensearch.client.indices.CreateIndexRequest
import java.util.concurrent.TimeUnit

abstract class BulkReplicationIT : MultiClusterRestTestCase() {

    abstract val indexPrefix: String
    abstract val operationName: String

    abstract fun executeBulk(
        client: RestHighLevelClient,
        pattern: String,
        excludeIndices: List<String> = emptyList()
    ): Map<String, Any>

    abstract fun assertPostOperationState(client: RestHighLevelClient, index: String)

    open fun setupPreCondition(client: RestHighLevelClient, pattern: String) {}

    open fun cleanup(client: RestHighLevelClient, pattern: String) {
        // Retry stop in case a previous bulk task is still completing (lock release is async)
        var attempts = 0
        while (attempts < 10) {
            try {
                client.bulkStopReplication(pattern)
                return
            } catch (e: org.opensearch.client.ResponseException) {
                if (e.message?.contains("already running") == true || e.message?.contains("CONFLICT") == true) {
                    attempts++
                    Thread.sleep(3000)
                } else {
                    throw e
                }
            }
        }
        client.bulkStopReplication(pattern) // final attempt, let it throw
    }

    protected fun createAndStartReplication(
        leaderClient: RestHighLevelClient,
        followerClient: RestHighLevelClient,
        suffix: String,
        count: Int = 3
    ) {
        for (i in 1..count) {
            leaderClient.indices().create(CreateIndexRequest("$indexPrefix-$suffix-$i"), RequestOptions.DEFAULT)
            followerClient.startReplication(
                StartReplicationRequest("source", "$indexPrefix-$suffix-$i", "$indexPrefix-$suffix-$i"),
                waitForRestore = true
            )
        }
        // Brief wait for cluster state to propagate replication state params to all nodes
        Thread.sleep(10000)
    }

    fun `test bulk operation in following state`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        createAndStartReplication(leaderClient, followerClient, "base")
        setupPreCondition(followerClient, "$indexPrefix-base-*")

        val response = executeBulk(followerClient, "$indexPrefix-base-*")
        `validate bulk response`(response)
        val statusResp = followerClient.waitForBulkTaskCompletion(response["task_id"].toString()) ?: return
        `validate task status response`(statusResp, operationName, "$indexPrefix-base-*")
        assertThat(statusResp["num_success"]).isEqualTo(3)

        assertBusy({
            for (i in 1..3) assertPostOperationState(followerClient, "$indexPrefix-base-$i")
        }, 30, TimeUnit.SECONDS)

        cleanup(followerClient, "$indexPrefix-base-*")
    }

    fun `test bulk operation without replication in progress`() {
        val followerClient = getClientForCluster(FOLLOWER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        assertThatThrownBy {
            executeBulk(followerClient, "non-existent-*")
        }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining("404")
    }

    fun `test bulk operation with exclude index filter`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        createAndStartReplication(leaderClient, followerClient, "excl")
        setupPreCondition(followerClient, "$indexPrefix-excl-*")

        val response = executeBulk(followerClient, "$indexPrefix-excl-*", listOf("$indexPrefix-excl-3"))
        `validate bulk response`(response)
        val s = followerClient.waitForBulkTaskCompletion(response["task_id"].toString()) ?: return
        assertThat(s["num_success"]).isEqualTo(2)

        cleanup(followerClient, "$indexPrefix-excl-*")
    }

    fun `test bulk operation task status response`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        createAndStartReplication(leaderClient, followerClient, "status")
        setupPreCondition(followerClient, "$indexPrefix-status-*")

        val response = executeBulk(followerClient, "$indexPrefix-status-*")
        val statusResp = followerClient.waitForBulkTaskCompletion(response["task_id"].toString()) ?: return
        assertThat(statusResp["operation_type"]).isEqualTo(operationName)
        assertThat(statusResp["num_success"]).isEqualTo(3)
        assertThat(statusResp["num_failed"]).isEqualTo(0)
        assertThat(statusResp["num_pending"]).isEqualTo(0)
        assertThat(statusResp["num_cancelled"]).isEqualTo(0)
        assertThat(statusResp["failed_indices"] as List<*>).isEmpty()

        cleanup(followerClient, "$indexPrefix-status-*")
    }
}
