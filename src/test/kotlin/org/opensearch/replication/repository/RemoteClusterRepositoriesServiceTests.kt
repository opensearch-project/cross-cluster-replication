package org.opensearch.replication.repository

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
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

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class RemoteClusterRepositoriesServiceTests : OpenSearchTestCase() {
    var threadPool = TestThreadPool("ReplicationPluginTest")

    fun `test changes in seed_nodes`() {
        var clusterSetting = ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        val discoveryNode = DiscoveryNode(
                "node",
                buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES,
                Version.CURRENT
        )
        var clusterService  = ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSetting)
        val repositoriesService = Mockito.mock(RepositoriesService::class.java)
        RemoteClusterRepositoriesService(Supplier { repositoriesService }, clusterService)
        clusterSetting.applySettings(Settings.builder().putList("cluster.remote.con-alias.seeds", "127.0.0.1:9300", "127.0.0.2:9300").build())
        Mockito.verify(repositoriesService).registerInternalRepository(REMOTE_REPOSITORY_PREFIX + "con-alias", REMOTE_REPOSITORY_TYPE)
    }

    fun `test changes in proxy_id for proxy-setup`() {
        var clusterSetting = ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        val discoveryNode = DiscoveryNode(
                "node",
                buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES,
                Version.CURRENT
        )
        var clusterService  = ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSetting)
        val repositoriesService = Mockito.mock(RepositoriesService::class.java)
        RemoteClusterRepositoriesService(Supplier { repositoriesService }, clusterService)
        clusterSetting.applySettings(Settings.builder().put("cluster.remote.con-alias.mode", "proxy").put("cluster.remote.con-alias.proxy_address", "127.0.0.1:100").build())
        Mockito.verify(repositoriesService).registerInternalRepository(REMOTE_REPOSITORY_PREFIX + "con-alias", REMOTE_REPOSITORY_TYPE)
    }
}
