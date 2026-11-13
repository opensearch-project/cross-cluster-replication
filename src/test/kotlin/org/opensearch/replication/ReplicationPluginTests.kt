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

import org.mockito.Mockito
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.core.common.io.stream.NamedWriteableRegistry
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.env.Environment
import org.opensearch.identity.PluginSubject
import org.opensearch.repositories.RepositoriesService
import org.opensearch.script.ScriptService
import org.opensearch.test.ClusterServiceUtils
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.client.NoOpClient
import org.opensearch.threadpool.TestThreadPool
import org.opensearch.threadpool.ThreadPool
import org.opensearch.watcher.ResourceWatcherService
import java.util.concurrent.TimeUnit
import java.util.function.Supplier

class ReplicationPluginTests : OpenSearchTestCase() {

    fun testAssignSubjectSetsTheSubjectOnThePluginClient() {
        val pluginSettings = ReplicationPlugin().settings.filter { it.properties.contains(Setting.Property.NodeScope) }
        val clusterSettings = ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS + pluginSettings)
        val threadPool = TestThreadPool(ReplicationPluginTests::class.java.name)
        try {
            ClusterServiceUtils.createClusterService(Settings.EMPTY, clusterSettings, threadPool).use { clusterService ->
                val plugin = ReplicationPlugin()
                newNodeEnvironment().use { nodeEnvironment ->
                    plugin.createComponents(NoOpClient(threadPool), clusterService, threadPool,
                            Mockito.mock(ResourceWatcherService::class.java), Mockito.mock(ScriptService::class.java),
                            NamedXContentRegistry.EMPTY, Mockito.mock(Environment::class.java), nodeEnvironment,
                            NamedWriteableRegistry(emptyList()), Mockito.mock(IndexNameExpressionResolver::class.java),
                            Supplier<RepositoriesService> { throw AssertionError("not used by createComponents") })
                }
                val subject = Mockito.mock(PluginSubject::class.java)

                plugin.assignSubject(subject)
                plugin.assignSubject(subject)

                // Only a client that already holds the first subject rejects a second one.
                val e = expectThrows(IllegalStateException::class.java) { plugin.assignSubject(Mockito.mock(PluginSubject::class.java)) }
                assertTrue(e.message.orEmpty().contains("already has a subject assigned"))
            }
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS)
        }
    }
}
