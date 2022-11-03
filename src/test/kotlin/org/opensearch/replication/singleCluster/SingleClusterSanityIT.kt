package org.opensearch.replication.singleCluster


import org.apache.logging.log4j.LogManager
import org.junit.BeforeClass
import org.opensearch.client.ResponseException
import org.opensearch.replication.MultiClusterRestTestCase
import org.opensearch.replication.StartReplicationRequest
import org.opensearch.replication.startReplication
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.Assert
import org.opensearch.client.Request
import org.opensearch.replication.stopReplication
import java.util.stream.Collectors




class SingleClusterSanityIT : MultiClusterRestTestCase() {

    companion object {
        private val log = LogManager.getLogger(SingleClusterSanityIT::class.java)
        private const val followerClusterName = "followCluster"
        private const val REPLICATION_PLUGIN_NAME = "opensearch-cross-cluster-replication"
        private const val SAMPLE_INDEX = "sample_test_index"

        @BeforeClass
        @JvmStatic
        fun setupTestClusters() {
            val clusters = HashMap<String, TestCluster>()
            clusters.put(followerClusterName, createTestCluster(followerClusterName, true, true, true, false))
            testClusters = clusters
        }

        enum class ClusterState(val value: String) {
            SINGLE_CLUSTER_SANITY_SUITE("integTestSingleCluster");

            companion object {
                fun from(s: String): ClusterState? = values().find { it.value == s }
            }
        }
    }

    @Throws(Exception::class)
    fun testReplicationPluginWithSingleCluster() {
        when(ClusterState.from(System.getProperty("tests.sanitySingleCluster"))) {
            ClusterState.SINGLE_CLUSTER_SANITY_SUITE -> basicReplicationSanityWithSingleCluster()
        }
    }

    fun basicReplicationSanityWithSingleCluster() {
        verifyReplicationPluginInstallationOnAllNodes(followerClusterName)
        VerifyReplicationApis(followerClusterName)
    }

    @Throws(java.lang.Exception::class)
    private fun verifyReplicationPluginInstallationOnAllNodes(clusterName: String) {
        val restClient = getClientForCluster(clusterName)
        val nodes = getAsList(restClient.lowLevelClient, "_cat/nodes?format=json") as List<Map<String, String>>
        nodes.forEach { node ->
            val nodeName = node["name"]
            val responseMap = getAsMap(restClient.lowLevelClient, "_nodes/$nodeName/plugins")["nodes"]
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

    @Throws(java.lang.Exception::class)
    private fun VerifyReplicationApis(clusterName: String) {
        val follower = getClientForCluster(followerClusterName)
        assertThatThrownBy {
            follower.startReplication(
                StartReplicationRequest("sample_connection", SAMPLE_INDEX, SAMPLE_INDEX),
                waitForRestore = true
            )
        }.isInstanceOf(ResponseException::class.java).hasMessageContaining("no such remote cluster")
        assertThatThrownBy {
            follower.stopReplication(followerClusterName)
        }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining("No replication in progress for index:"+followerClusterName)
    }

}

