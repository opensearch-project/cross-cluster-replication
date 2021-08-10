/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
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