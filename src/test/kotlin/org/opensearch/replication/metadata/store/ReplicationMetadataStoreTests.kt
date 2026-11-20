/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.replication.metadata.store

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import kotlinx.coroutines.runBlocking
import org.mockito.Mockito
import org.opensearch.ResourceNotFoundException
import org.opensearch.Version
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionType
import org.opensearch.action.admin.cluster.health.ClusterHealthAction
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse
import org.opensearch.action.admin.indices.create.CreateIndexAction
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.action.admin.indices.mapping.put.PutMappingAction
import org.opensearch.action.bulk.BulkAction
import org.opensearch.action.bulk.BulkItemResponse
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.delete.DeleteAction
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.get.GetAction
import org.opensearch.action.get.GetResponse
import org.opensearch.action.get.MultiGetAction
import org.opensearch.action.get.MultiGetItemResponse
import org.opensearch.action.get.MultiGetResponse
import org.opensearch.action.index.IndexAction
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.health.ClusterHealthStatus
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.routing.RoutingTable
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.CheckedRunnable
import org.opensearch.common.settings.Settings
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.identity.NamedPrincipal
import org.opensearch.identity.PluginSubject
import org.opensearch.index.get.GetResult
import org.opensearch.replication.metadata.ReplicationOverallState
import org.opensearch.replication.util.PluginClient
import org.opensearch.test.ClusterServiceUtils
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.client.NoOpClient
import org.opensearch.threadpool.TestThreadPool
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client
import java.security.Principal
import java.util.Collections
import java.util.concurrent.TimeUnit

/** Checks that every request the store sends on its system index runs as the plugin subject. */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class ReplicationMetadataStoreTests : OpenSearchTestCase() {

    companion object {
        private const val IDENTITY = "identity"
        private const val CALLER = "caller"
        private const val SUBJECT = "plugin"
        private const val INDEX = ReplicationMetadataStore.REPLICATION_CONFIG_SYSTEM_INDEX
        private val TYPE = ReplicationStoreMetadataType.INDEX.name
    }

    private lateinit var threadPool: ThreadPool
    private lateinit var clusterService: ClusterService
    private val calls: MutableList<String> = Collections.synchronizedList(mutableListOf())

    override fun setUp() {
        super.setUp()
        threadPool = TestThreadPool(javaClass.simpleName)
        clusterService = ClusterServiceUtils.createClusterService(threadPool)
        threadPool.threadContext.putTransient(IDENTITY, CALLER)
    }

    override fun tearDown() {
        clusterService.close()
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS)
        super.tearDown()
    }

    fun testReadsAndWritesOnAnExistingStoreRunAsTheSubject() {
        ClusterServiceUtils.setState(clusterService, stateWithStore())
        val store = ReplicationMetadataStore(pluginClient(), clusterService, NamedXContentRegistry.EMPTY)
        val get = GetReplicationMetadataRequest(TYPE, null, "follower")

        runBlocking {
            store.addMetadata(AddReplicationMetadataRequest(metadata()))
            expectNotFound { runBlocking { store.getMetadata(get, false) } }
            store.getMetadata(TYPE, listOf("follower"))
            store.deleteMetadata(DeleteReplicationMetadataRequest(TYPE, null, "follower"))
            store.deleteMetadata(TYPE, listOf("follower"))
            store.updateMetadata(UpdateReplicationMetadataRequest(metadata(), 1, 1))
        }
        // The coroutine elements leave a default context behind, so the blocking read starts from the caller again.
        threadPool.threadContext.putTransient(IDENTITY, CALLER)
        expectNotFound { store.getMetadata(get, false, 10_000) }

        val health = ClusterHealthAction.NAME
        assertEquals(
            listOf(
                health, PutMappingAction.NAME, IndexAction.NAME,
                health, GetAction.NAME,
                health, MultiGetAction.NAME,
                health, DeleteAction.NAME,
                health, BulkAction.NAME,
                health, PutMappingAction.NAME, IndexAction.NAME,
                health, GetAction.NAME
            ).map { "$it as $SUBJECT" },
            calls
        )
        assertEquals("context after the blocking read", CALLER, threadPool.threadContext.getTransient<String>(IDENTITY))
    }

    fun testCreatingTheStoreRunsAsTheSubject() {
        val store = ReplicationMetadataStore(pluginClient(), clusterService, NamedXContentRegistry.EMPTY)

        // The store index never appears in this cluster state, so the mapping check after the create fails.
        expectNotFound { runBlocking { store.addMetadata(AddReplicationMetadataRequest(metadata())) } }

        assertEquals(listOf("${CreateIndexAction.NAME} as $SUBJECT"), calls)
    }

    private fun expectNotFound(block: () -> Unit) {
        expectThrows(ResourceNotFoundException::class.java) { block() }
    }

    private fun pluginClient(): Client {
        val pluginClient = PluginClient(RecordingClient(threadPool))
        pluginClient.setSubject(MarkingSubject(threadPool))
        return pluginClient
    }

    private fun metadata(): ReplicationMetadata = ReplicationMetadata(
        "conn", TYPE, ReplicationOverallState.RUNNING.name, "reason",
        ReplicationContext("follower"), ReplicationContext("leader"), Settings.EMPTY
    )

    private fun stateWithStore(): ClusterState {
        val index = IndexMetadata.builder(INDEX)
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build()
        return ClusterState.builder(clusterService.state())
            .metadata(Metadata.builder(clusterService.state().metadata()).put(index, false))
            .routingTable(RoutingTable.builder().addAsNew(index).build())
            .build()
    }

    /** A plugin subject that marks the context it runs in, the way the security plugin's subject replaces the user. */
    private class MarkingSubject(private val threadPool: ThreadPool) : PluginSubject {
        override fun getPrincipal(): Principal = NamedPrincipal(SUBJECT)

        override fun <E : Exception> runAs(r: CheckedRunnable<E>) {
            threadPool.threadContext.stashContext().use {
                threadPool.threadContext.putTransient(IDENTITY, SUBJECT)
                r.run()
            }
        }
    }

    /** Records each action with the identity in context, and answers it from a canned response. */
    private inner class RecordingClient(threadPool: ThreadPool) : NoOpClient(threadPool) {
        @Suppress("UNCHECKED_CAST")
        override fun <Request : ActionRequest, Response : ActionResponse> doExecute(
            action: ActionType<Response>,
            request: Request,
            listener: ActionListener<Response>
        ) {
            calls.add("${action.name()} as ${threadPool().threadContext.getTransient<String>(IDENTITY)}")
            val response: ActionResponse = when (action.name()) {
                ClusterHealthAction.NAME -> Mockito.mock(ClusterHealthResponse::class.java).also {
                    Mockito.`when`(it.status).thenReturn(ClusterHealthStatus.GREEN)
                }
                GetAction.NAME -> GetResponse(GetResult(INDEX, "follower", -2, 0, -1, false, null, null, null))
                MultiGetAction.NAME -> MultiGetResponse(arrayOf<MultiGetItemResponse>())
                DeleteAction.NAME -> Mockito.mock(DeleteResponse::class.java)
                BulkAction.NAME -> BulkResponse(arrayOf<BulkItemResponse>(), 0)
                IndexAction.NAME -> Mockito.mock(IndexResponse::class.java)
                PutMappingAction.NAME -> AcknowledgedResponse(true)
                CreateIndexAction.NAME -> CreateIndexResponse(true, true, INDEX)
                else -> throw AssertionError("unexpected action ${action.name()}")
            }
            listener.onResponse(response as Response)
        }
    }
}
