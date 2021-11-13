package org.opensearch.replication.bwc;

import org.assertj.core.api.Assertions
import org.junit.Assert
import org.opensearch.action.DocWriteResponse
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.get.GetRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.replication.MultiClusterAnnotations
import org.opensearch.replication.MultiClusterRestTestCase
import org.opensearch.replication.StartReplicationRequest
import org.opensearch.replication.startReplication
import org.opensearch.test.OpenSearchTestCase.assertBusy
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors


const val LEADER = "bwcLeader"
const val FOLLOWER = "bwcFollower"
const val NUM_NODES = 3
const val REPLICATION_PLUGIN_NAME = "opensearch-cross-cluster-replication"
const val LEADER_INDEX = "bwc_test_index"
const val FOLLOWER_INDEX = "bwc_test_index"
const val CONNECTION_NAME = "bwc_connection"

/*
    Verifies that replication is working with following upgrade scenarios:
    - mixed cluster: where only one of the nodes in cluster has upgraded to the new version.
    - rolling restart: where all nodes have upgraded to the latest version one by one.
    - full cluster restart: where all nodes have simultaneously upgraded to the latest version.
    - bwcTestSuite: Runs all 3 scenarios above.

    Here is the tasks order for each scenario:
    - mixed cluster:  oldVersionClusterTask0 --> mixedClusterTask
    - rolling restart:  oldVersionClusterTask0 --> mixedClusterTask -> twoThirdsUpgradedClusterTask -> rollingUpgradeClusterTask
    - full cluster restart:  oldVersionClusterTask1 --> fullRestartClusterTask
 */
@MultiClusterAnnotations.ClusterConfigurations(
    MultiClusterAnnotations.ClusterConfiguration(clusterName = "${LEADER}0", preserveIndices = true, preserveClusterSettings = true),
    MultiClusterAnnotations.ClusterConfiguration(clusterName = "${FOLLOWER}0", preserveIndices = true, preserveClusterSettings = true)
)
class BackwardsCompatibilityIT : MultiClusterRestTestCase() {
    private val clusterSuffix = System.getProperty("tests.cluster_suffix")
    private val leaderName = "${LEADER}$clusterSuffix"
    private val followerName = "${FOLLOWER}$clusterSuffix"


    enum class ClusterStatus(val value: String) {
        OLD("oldVersionClusterTask"),
        ONE_THIRD_UPGRADED("mixedClusterTask"),
        TWO_THIRD_UPGRADED("twoThirdsUpgradedClusterTask"),
        ROLLING_UPGRADED("rollingUpgradeClusterTask"),
        FULL_CLUSTER_RESTART("fullRestartClusterTask"),
        COMPLETE_SUITE("bwcTestSuite");
        companion object {
            fun from(s: String): ClusterStatus? = values().find { it.value == s }
        }
    }

    @Throws(Exception::class)
    fun testReplicationPlugin() {
        when(ClusterStatus.from(System.getProperty("tests.bwcTask"))) {
            ClusterStatus.OLD -> setupReplication()
            ClusterStatus.ONE_THIRD_UPGRADED, ClusterStatus.TWO_THIRD_UPGRADED, ClusterStatus.ROLLING_UPGRADED,
                ClusterStatus.FULL_CLUSTER_RESTART -> verifyReplication()
            ClusterStatus.COMPLETE_SUITE -> {} // Do nothing as all tests have run already
        }
    }

    // Set up the replication between two clusters.
    private fun setupReplication() {
        val follower = getClientForCluster(followerName)
        val leader = getClientForCluster(leaderName)

        // Verify that both clusters are up.
        leader.cluster().health(ClusterHealthRequest(), RequestOptions.DEFAULT)
        follower.cluster().health(ClusterHealthRequest(), RequestOptions.DEFAULT)

        createConnectionBetweenClusters(followerName, leaderName, CONNECTION_NAME)

        try {
            // Create an empty index on the leader and trigger replication on it
            val createIndexResponse = leader.indices().create(CreateIndexRequest(LEADER_INDEX), RequestOptions.DEFAULT)
            Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()

            follower.startReplication(StartReplicationRequest(CONNECTION_NAME, LEADER_INDEX, FOLLOWER_INDEX), waitForRestore=true)

            val source = mapOf("name" to randomAlphaOfLength(20), "age" to randomInt().toString())
            var response = leader.index(IndexRequest(LEADER_INDEX).id("1").source(source), RequestOptions.DEFAULT)
            Assertions.assertThat(response.result).isEqualTo(DocWriteResponse.Result.CREATED)

            assertBusy({
                val getResponse = follower.get(GetRequest(FOLLOWER_INDEX, "1"), RequestOptions.DEFAULT)
                Assertions.assertThat(getResponse.isExists).isTrue()
                Assertions.assertThat(getResponse.sourceAsMap).isEqualTo(source)
            }, 60, TimeUnit.SECONDS)
        } catch (e: Exception) {
            logger.info("Exception while starting the replication ${e.printStackTrace()}")
            throw e
        }
    }

    // Verifies that replication is still ongoing.
    private fun verifyReplication() {
        verifyReplicationPluginInstalled(leaderName)
        verifyReplicationPluginInstalled(followerName)

        val follower = getClientForCluster(followerName)
        val leader = getClientForCluster(leaderName)

        // Update the seed nodes.
        createConnectionBetweenClusters(followerName, leaderName, CONNECTION_NAME)

        try {
            val id = randomInt().toString()
            // Ensures that this document ID isn't already present
            leader.delete(DeleteRequest(LEADER_INDEX).id(id), RequestOptions.DEFAULT)

            val source = mapOf("name" to randomAlphaOfLength(20), "age" to randomInt().toString())
            var response = leader.index(IndexRequest(LEADER_INDEX).id(id).source(source), RequestOptions.DEFAULT)
            Assertions.assertThat(response.result).isEqualTo(DocWriteResponse.Result.CREATED)

            assertBusy({
                val getResponse = follower.get(GetRequest(FOLLOWER_INDEX, id), RequestOptions.DEFAULT)
                Assertions.assertThat(getResponse.isExists).isTrue()
                Assertions.assertThat(getResponse.sourceAsMap).isEqualTo(source)
            }, 60, TimeUnit.SECONDS)
        } catch (e: Exception) {
            logger.info("Exception while verifying the replication ${e.printStackTrace()}")
            throw e
        }
    }

    // Verifies that replication plugin is installed on all the nodes og the cluster.
    @Throws(java.lang.Exception::class)
    private fun verifyReplicationPluginInstalled(clusterName: String) {
        val restClient = getClientForCluster(clusterName)
        for (i in 0 until NUM_NODES) {
            val responseMap = getAsMap(restClient.lowLevelClient, "_nodes/$clusterName-$i/plugins")["nodes"]
                    as Map<String, Map<String, Any>>?
            Assert.assertTrue(responseMap!!.values.isNotEmpty())
            for (response in responseMap!!.values) {
                val plugins = response["plugins"] as List<Map<String, Any>>?
                val pluginNames: Set<Any?> = plugins!!.stream().map { map: Map<String, Any> ->
                    map["name"]
                }.collect(Collectors.toSet()).orEmpty()
                Assert.assertTrue(pluginNames.contains(REPLICATION_PLUGIN_NAME))
            }
        }
    }
}
