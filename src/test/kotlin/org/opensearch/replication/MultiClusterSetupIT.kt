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

package org.opensearch.replication

import org.opensearch.replication.MultiClusterAnnotations.ClusterConfiguration
import org.apache.http.util.EntityUtils
import org.opensearch.client.Request

@MultiClusterAnnotations.ClusterConfigurations(
    ClusterConfiguration(clusterName = "leaderCluster"),
    ClusterConfiguration(clusterName = "followCluster")
)
class MultiClusterSetupIT : MultiClusterRestTestCase() {

    fun testRepPluginLoadedOnLeaderCluster() {
        val restClientForLeader = getNamedCluster("leaderCluster").lowLevelClient
        val installedPlugins = getAsMap(restClientForLeader, "_nodes/plugins")
        val nodes = installedPlugins["nodes"] as Map<String, Map<String, Any>>?
        for (node in nodes!!.values) {
            val nodePlugins = node["plugins"] as List<Map<String, Any>>?
            assertTrue("Cross cluster plugin wasn't installed on node: " + node["name"],
                       isReplicationPluginInstalledOnNode(nodePlugins))
        }
    }

    fun testRepPluginInstalledOnFollowerCluster() {
        val restClientForLeader = getNamedCluster("followCluster").lowLevelClient
        val installedPlugins = getAsMap(restClientForLeader, "_nodes/plugins")
        val nodes = installedPlugins["nodes"] as Map<String, Map<String, Any>>?
        for (node in nodes!!.values) {
            val nodePlugins = node["plugins"] as List<Map<String, Any>>?
            assertTrue("Cross cluster plugin wasn't installed on node: " + node["name"],
                       isReplicationPluginInstalledOnNode(nodePlugins))
        }
    }

    private fun isReplicationPluginInstalledOnNode(nodePlugins: List<Map<String, Any>>?): Boolean {
        for (plugin in nodePlugins!!) if (plugin["name"] == "opensearch-cross-cluster-replication") return true
        return false
    }

    fun testClusterConnection() {
        createConnectionBetweenClusters("followCluster", "leaderCluster")
        val getSettingsRequest = Request("GET", "/_cluster/settings")
        val settingsResponse = getNamedCluster("followCluster").lowLevelClient.performRequest(getSettingsRequest)
        val responseString = EntityUtils.toString(settingsResponse.entity)
        assertTrue(responseString.contains("remote"))
        assertTrue(responseString.contains(getNamedCluster("leaderCluster").transportPorts[0]))
    }
}