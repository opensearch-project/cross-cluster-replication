/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.replication.repository

import com.nhaarman.mockitokotlin2.times
import org.mockito.Mockito
import org.opensearch.Version
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.node.DiscoveryNodeRole
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Settings
import org.opensearch.repositories.RepositoriesService
import org.opensearch.test.ClusterServiceUtils
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.TestThreadPool
import java.util.function.Supplier

class RemoteClusterRepositoriesServiceTests : OpenSearchTestCase() {

    fun `test changes in seed_nodes`() {
        var clusterSetting = ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        var threadPool = TestThreadPool("ReplicationPluginTest")
        val discoveryNode = DiscoveryNode(
            "node",
            buildNewFakeTransportAddress(), emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT,
        )
        var clusterService = ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSetting)
        val repositoriesService = Mockito.mock(RepositoriesService::class.java)
        RemoteClusterRepositoriesService(Supplier { repositoriesService }, clusterService)
        clusterSetting.applySettings(Settings.builder().putList("cluster.remote.con-alias.seeds", "127.0.0.1:9300", "127.0.0.2:9300").build())
        Mockito.verify(repositoriesService, times(1)).registerInternalRepository(REMOTE_REPOSITORY_PREFIX + "con-alias", REMOTE_REPOSITORY_TYPE)
        threadPool.shutdown()
    }

    fun `test removal of seed_nodes`() {
        var clusterSetting = ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        var threadPool = TestThreadPool("ReplicationPluginTest")
        val discoveryNode = DiscoveryNode(
            "node",
            buildNewFakeTransportAddress(), emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT,
        )
        var clusterService = ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSetting)
        val repositoriesService = Mockito.mock(RepositoriesService::class.java)
        RemoteClusterRepositoriesService(Supplier { repositoriesService }, clusterService)
        clusterSetting.applySettings(Settings.builder().putList("cluster.remote.con-alias.seeds", "127.0.0.1:9300", "127.0.0.2:9300").build())
        Mockito.verify(repositoriesService, times(1)).registerInternalRepository(REMOTE_REPOSITORY_PREFIX + "con-alias", REMOTE_REPOSITORY_TYPE)
        clusterSetting.applySettings(Settings.builder().putNull("cluster.remote.con-alias.seeds").build())
        Mockito.verify(repositoriesService, times(1)).unregisterInternalRepository(REMOTE_REPOSITORY_PREFIX + "con-alias")
        threadPool.shutdown()
    }

    fun `test changes in proxy_id for proxy-setup`() {
        var clusterSetting = ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        var threadPool = TestThreadPool("ReplicationPluginTest")
        val discoveryNode = DiscoveryNode(
            "node",
            buildNewFakeTransportAddress(), emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT,
        )
        var clusterService = ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSetting)
        val repositoriesService = Mockito.mock(RepositoriesService::class.java)
        RemoteClusterRepositoriesService(Supplier { repositoriesService }, clusterService)
        clusterSetting.applySettings(Settings.builder().put("cluster.remote.con-alias.mode", "proxy").put("cluster.remote.con-alias.proxy_address", "127.0.0.1:100").build())
        Mockito.verify(repositoriesService, times(1)).registerInternalRepository(REMOTE_REPOSITORY_PREFIX + "con-alias", REMOTE_REPOSITORY_TYPE)
        threadPool.shutdown()
    }

    fun `test removal of proxy_id for proxy-setup`() {
        var clusterSetting = ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        var threadPool = TestThreadPool("ReplicationPluginTest")
        val discoveryNode = DiscoveryNode(
            "node",
            buildNewFakeTransportAddress(), emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT,
        )
        var clusterService = ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSetting)
        val repositoriesService = Mockito.mock(RepositoriesService::class.java)
        RemoteClusterRepositoriesService(Supplier { repositoriesService }, clusterService)
        clusterSetting.applySettings(Settings.builder().put("cluster.remote.con-alias.mode", "proxy").put("cluster.remote.con-alias.proxy_address", "127.0.0.1:100").build())
        Mockito.verify(repositoriesService, times(1)).registerInternalRepository(REMOTE_REPOSITORY_PREFIX + "con-alias", REMOTE_REPOSITORY_TYPE)
        clusterSetting.applySettings(Settings.builder().putNull("cluster.remote.con-alias.mode").build())
        clusterSetting.applySettings(Settings.builder().putNull("cluster.remote.con-alias.proxy_address").build())
        Mockito.verify(repositoriesService, times(1)).unregisterInternalRepository(REMOTE_REPOSITORY_PREFIX + "con-alias")
        threadPool.shutdown()
    }
}
