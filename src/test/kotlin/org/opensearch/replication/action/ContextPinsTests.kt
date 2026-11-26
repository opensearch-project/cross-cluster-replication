/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.replication.action

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.mockito.Mockito
import org.opensearch.Version
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionType
import org.opensearch.action.admin.cluster.state.ClusterStateAction
import org.opensearch.action.admin.cluster.state.ClusterStateResponse
import org.opensearch.action.admin.indices.get.GetIndexAction
import org.opensearch.action.admin.indices.get.GetIndexResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.metadata.MetadataCreateIndexService
import org.opensearch.cluster.metadata.RepositoryMetadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.IndexScopedSettings
import org.opensearch.common.settings.Settings
import org.opensearch.commons.ConfigConstants
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.core.index.Index
import org.opensearch.core.tasks.TaskId
import org.opensearch.env.Environment
import org.opensearch.indices.recovery.RecoverySettings
import org.opensearch.persistent.PersistentTaskParams
import org.opensearch.persistent.PersistentTaskResponse
import org.opensearch.persistent.PersistentTasksCustomMetadata
import org.opensearch.persistent.PersistentTasksService
import org.opensearch.persistent.RemovePersistentTaskAction
import org.opensearch.repositories.RepositoriesService
import org.opensearch.transport.client.node.NodeClient
import org.opensearch.replication.ReplicationSettings
import org.opensearch.replication.action.autofollow.AutoFollowClusterManagerNodeAction
import org.opensearch.replication.action.autofollow.AutoFollowClusterManagerNodeRequest
import org.opensearch.replication.action.autofollow.TransportUpdateAutoFollowPatternAction
import org.opensearch.replication.action.autofollow.UpdateAutoFollowPatternRequest
import org.opensearch.replication.action.bulk.BulkReplicationRequest
import org.opensearch.replication.action.bulk.BulkReplicationResponse
import org.opensearch.replication.metadata.state.BulkTaskState
import org.opensearch.replication.action.bulk.TransportBulkPauseReplicationAction
import org.opensearch.replication.action.bulk.TransportBulkReplicationAction
import org.opensearch.replication.action.bulk.TransportBulkResumeReplicationAction
import org.opensearch.replication.action.bulk.TransportBulkStartReplicationAction
import org.opensearch.replication.action.bulk.TransportBulkStopReplicationAction
import org.opensearch.replication.action.bulk.UpdateBulkTaskStateAction
import org.opensearch.replication.action.index.ReplicateIndexClusterManagerNodeAction
import org.opensearch.replication.action.index.ReplicateIndexClusterManagerNodeRequest
import org.opensearch.replication.action.index.ReplicateIndexRequest
import org.opensearch.replication.action.index.TransportReplicateIndexAction
import org.opensearch.replication.action.index.TransportReplicateIndexClusterManagerNodeAction
import org.opensearch.replication.action.resume.TransportResumeIndexReplicationAction
import org.opensearch.replication.action.setup.SetupChecksAction
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.repository.REMOTE_REPOSITORY_PREFIX
import org.opensearch.replication.repository.RemoteClusterRepository
import org.opensearch.replication.task.bulk.BulkOperationType
import org.opensearch.replication.task.bulk.BulkReplicationTask
import org.opensearch.replication.task.index.IndexReplicationExecutor
import org.opensearch.replication.task.index.IndexReplicationParams
import org.opensearch.tasks.Task
import org.opensearch.tasks.TaskManager
import org.opensearch.test.ClusterServiceUtils
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.client.NoOpClient
import org.opensearch.threadpool.TestThreadPool
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import java.util.Collections
import java.util.concurrent.TimeUnit

/**
 * Checks the requests that are pinned to a default context, and the coroutine contexts of the bulk action and the
 * bulk task. Each test fails when its pin is removed.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class ContextPinsTests : OpenSearchTestCase() {

    companion object {
        private const val IDENTITY = "identity"
        private const val CALLER = "caller"
    }

    private lateinit var threadPool: ThreadPool
    private lateinit var clusterService: ClusterService
    private lateinit var client: RecordingClient
    private val transportService: TransportService = Mockito.mock(TransportService::class.java)

    override fun setUp() {
        super.setUp()
        threadPool = TestThreadPool(javaClass.simpleName)
        clusterService = ClusterServiceUtils.createClusterService(threadPool)
        client = RecordingClient(threadPool)
        Mockito.`when`(transportService.taskManager).thenReturn(TaskManager(Settings.EMPTY, threadPool, emptySet()))
        threadPool.threadContext.putTransient(IDENTITY, CALLER)
    }

    override fun tearDown() {
        clusterService.close()
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS)
        super.tearDown()
    }

    fun testStartOnTheClusterManagerRunsInADefaultContext() {
        val action = replicateIndexAction()
        runBlocking { action.startOnClusterManager(ReplicateIndexClusterManagerNodeRequest(null, ReplicateIndexRequest("f", "remote", "l"))) }
        assertEquals(listOf("${ReplicateIndexClusterManagerNodeAction.NAME} as null"), client.calls)
    }

    fun testAutoFollowGoesInADefaultContextAfterTheSetupChecksOnly() {
        val action = TransportUpdateAutoFollowPatternAction(transportService, threadPool, ActionFilters(emptySet()), client)
        val add = UpdateAutoFollowPatternRequest("remote", "p", "l*", UpdateAutoFollowPatternRequest.Action.ADD, Settings.EMPTY)
        val remove = UpdateAutoFollowPatternRequest("remote", "p", null, UpdateAutoFollowPatternRequest.Action.REMOVE, Settings.EMPTY)
        runBlocking {
            action.forwardToClusterManager(AutoFollowClusterManagerNodeRequest(null, add), afterSetupChecks = true)
            action.forwardToClusterManager(AutoFollowClusterManagerNodeRequest(null, remove), afterSetupChecks = false)
        }
        assertEquals(listOf("${AutoFollowClusterManagerNodeAction.NAME} as null", "${AutoFollowClusterManagerNodeAction.NAME} as $CALLER"),
                client.calls)
    }

    fun testResumeReadsTheLeaderStateInADefaultContext() {
        runBlocking { resumeAction().getLeaderIndexMetadata("remote", "leader") }
        assertEquals(listOf("${ClusterStateAction.NAME} as null"), client.calls)
    }

    fun testTheBulkLockIsClaimedInADefaultContextOnlyAfterEarlierCalls() {
        val claim = BulkTaskState("", "bulk", "x*", 0, 0, 0, 0, 0, emptyList())
        val expected = mapOf(BulkOperationType.START to null, BulkOperationType.RESUME to null,
                BulkOperationType.STOP to CALLER, BulkOperationType.PAUSE to CALLER)
        for ((type, identity) in expected) {
            client.calls.clear()
            runBlocking { bulkAction(type).claimLock(claim) }
            assertEquals(type.label, listOf("${UpdateBulkTaskStateAction.NAME} as $identity"), client.calls)
        }
    }

    fun testTheBulkLockIsReleasedInADefaultContext() {
        runBlocking { bulkStartAction().releaseLock() }
        assertEquals(listOf("${UpdateBulkTaskStateAction.NAME} as null"), client.calls)
    }

    fun testResumeRemovesItsFirstTaskAsTheCallerAndTheOthersInADefaultContext() {
        val tasks = PersistentTasksCustomMetadata.builder()
        for (id in listOf("replication:index:follower", "replication:[follower][0]")) {
            tasks.addTask<PersistentTaskParams>(id, IndexReplicationExecutor.TASK_NAME,
                    IndexReplicationParams("remote", Index("follower", "_na_"), "follower"), PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT)
        }
        ClusterServiceUtils.setState(clusterService, ClusterState.builder(clusterService.state())
                .metadata(Metadata.builder(clusterService.state().metadata()).putCustom(PersistentTasksCustomMetadata.TYPE, tasks.build())))
        runBlocking { resumeAction().removeExistingTasks("follower") }
        assertEquals(listOf("${RemovePersistentTaskAction.NAME} as $CALLER", "${RemovePersistentTaskAction.NAME} as null"), client.calls)
    }

    fun testAddingAPatternForwardsInADefaultContextAndRemovingOneAsTheCaller() {
        val action = TransportUpdateAutoFollowPatternAction(transportService, threadPool, ActionFilters(emptySet()), client)
        for (request in listOf(
                UpdateAutoFollowPatternRequest("remote", "p", "l*", UpdateAutoFollowPatternRequest.Action.ADD, Settings.EMPTY),
                UpdateAutoFollowPatternRequest("remote", "p", null, UpdateAutoFollowPatternRequest.Action.REMOVE, Settings.EMPTY))) {
            val done = CompletableDeferred<Unit>()
            action.execute(Mockito.mock(Task::class.java), request, object : ActionListener<AcknowledgedResponse> {
                override fun onResponse(response: AcknowledgedResponse) { done.complete(Unit) }
                override fun onFailure(e: Exception) { done.completeExceptionally(e) }
            })
            runBlocking { done.await() }
        }
        assertEquals(listOf("${SetupChecksAction.NAME} as $CALLER", "${AutoFollowClusterManagerNodeAction.NAME} as null",
                "${AutoFollowClusterManagerNodeAction.NAME} as $CALLER"), client.calls)
    }

    fun testTheBulkActionCompletesInTheCallersContextAfterAnInjectedCall() {
        val action = bulkStartAction()
        val request = BulkReplicationRequest("x*")
        request.leaderAlias = "remote"
        val seen = CompletableDeferred<List<String?>>()
        action.execute(Mockito.mock(Task::class.java), request, object : ActionListener<BulkReplicationResponse> {
            override fun onResponse(response: BulkReplicationResponse) { seen.complete(listOf("response")) }
            override fun onFailure(e: Exception) {
                val context = threadPool.threadContext
                seen.complete(listOf(context.getTransient(IDENTITY), context.getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_INJECTED_ROLES)))
            }
        })
        // No leader index matches, so the action fails after the setup checks and the leader index read.
        assertEquals(listOf(CALLER, null), runBlocking { seen.await() })
        assertEquals(listOf("${SetupChecksAction.NAME} as $CALLER", "${GetIndexAction.NAME} as null"), client.calls)
    }

    fun testTheBulkTaskRunsInADefaultContext() {
        val task = bulkTask()
        val seen = CompletableDeferred<String?>()
        task.run(object : ActionListener<org.opensearch.replication.task.bulk.BulkReplicationTaskStatus> {
            override fun onResponse(status: org.opensearch.replication.task.bulk.BulkReplicationTaskStatus) {
                seen.complete(threadPool.threadContext.getTransient(IDENTITY))
            }
            override fun onFailure(e: Exception) { seen.completeExceptionally(e) }
        }, "t1")
        assertNull("the task coroutine does not run in the caller's context", runBlocking { seen.await() })

    }

    fun testTheBulkTaskUpdatesItsStateInADefaultContext() {
        runBlocking { withCallerContext { bulkTask().updateClusterState() } }
        assertEquals(listOf("${UpdateBulkTaskStateAction.NAME} as null"), client.calls)
    }

    private fun bulkTask() = BulkReplicationTask(1, "transport", "bulk", "", TaskId.EMPTY_TASK_ID, emptyMap(),
            BulkReplicationRequest("x*"), BulkOperationType.STOP, client, clusterService, threadPool, 10, 1, emptyList(), emptyList(),
            bulkStartAction())

    fun testTheRestoreReadsTheLeaderStateInADefaultContext() {
        val repository = RemoteClusterRepository(RepositoryMetadata("${REMOTE_REPOSITORY_PREFIX}remote", "replication", Settings.EMPTY),
                client, clusterService, Mockito.mock(RecoverySettings::class.java), Mockito.mock(ReplicationMetadataManager::class.java),
                Mockito.mock(ReplicationSettings::class.java))
        repository.leaderClusterStateForRestore("leader")
        assertEquals(listOf("${ClusterStateAction.NAME} as null"), client.calls)
    }

    private suspend fun <T> withCallerContext(block: suspend () -> T): T {
        assertEquals(CALLER, threadPool.threadContext.getTransient<String>(IDENTITY))
        return block()
    }

    private fun bulkStartAction() = bulkAction(BulkOperationType.START)

    private fun bulkAction(type: BulkOperationType): TransportBulkReplicationAction {
        val resolver = IndexNameExpressionResolver(threadPool.threadContext)
        val manager = Mockito.mock(ReplicationMetadataManager::class.java)
        val settings = Mockito.mock(ReplicationSettings::class.java)
        val tasks = Mockito.mock(PersistentTasksService::class.java)
        val clusterManagerAction = TransportReplicateIndexClusterManagerNodeAction(transportService, clusterService, threadPool,
                ActionFilters(emptySet()), resolver, IndexScopedSettings.DEFAULT_SCOPED_SETTINGS, tasks,
                Mockito.mock(NodeClient::class.java), Mockito.mock(RepositoriesService::class.java), manager)
        val filters = ActionFilters(emptySet())
        return when (type) {
            BulkOperationType.START -> TransportBulkStartReplicationAction(transportService, filters, client, clusterService, threadPool,
                    resolver, manager, settings, tasks, replicateIndexAction(), clusterManagerAction)
            BulkOperationType.STOP -> TransportBulkStopReplicationAction(transportService, filters, client, clusterService, threadPool,
                    resolver, manager, settings, tasks, replicateIndexAction(), clusterManagerAction)
            BulkOperationType.PAUSE -> TransportBulkPauseReplicationAction(transportService, filters, client, clusterService, threadPool,
                    resolver, manager, settings, tasks, replicateIndexAction(), clusterManagerAction)
            BulkOperationType.RESUME -> TransportBulkResumeReplicationAction(transportService, filters, client, clusterService, threadPool,
                    resolver, manager, settings, tasks, replicateIndexAction(), clusterManagerAction)
        }
    }

    private fun resumeAction() = TransportResumeIndexReplicationAction(transportService, clusterService, threadPool,
            ActionFilters(emptySet()), IndexNameExpressionResolver(threadPool.threadContext), client,
            Mockito.mock(ReplicationMetadataManager::class.java), Mockito.mock(Environment::class.java))

    private fun replicateIndexAction() = TransportReplicateIndexAction(transportService, clusterService, threadPool,
            ActionFilters(emptySet()), client, Mockito.mock(Environment::class.java), Mockito.mock(MetadataCreateIndexService::class.java))

    /**
     * Records each action with the identity it is sent with, and answers on another thread in the context the request was
     * sent in, as the transport layer does.
     */
    private class RecordingClient(threadPool: ThreadPool) : NoOpClient(threadPool) {
        val calls: MutableList<String> = Collections.synchronizedList(mutableListOf())

        override fun getRemoteClusterClient(clusterAlias: String): Client = this

        @Suppress("UNCHECKED_CAST")
        override fun <Request : ActionRequest, Response : ActionResponse> doExecute(
            action: ActionType<Response>,
            request: Request,
            listener: ActionListener<Response>
        ) {
            calls.add("${action.name()} as ${threadPool().threadContext.getTransient<String>(IDENTITY)}")
            val response: ActionResponse = when (action.name()) {
                ClusterStateAction.NAME -> ClusterStateResponse(ClusterName("leader"), leaderState(), false)
                RemovePersistentTaskAction.NAME -> PersistentTaskResponse(null as PersistentTasksCustomMetadata.PersistentTask<*>?)
                GetIndexAction.NAME -> Mockito.mock(GetIndexResponse::class.java).also {
                    Mockito.`when`(it.indices).thenReturn(arrayOf())
                }
                else -> AcknowledgedResponse(true)
            }
            val sentIn = threadPool().threadContext.newRestorableContext(false)
            threadPool().generic().execute { sentIn.get().use { listener.onResponse(response as Response) } }
        }

        private fun leaderState(): ClusterState {
            val index = IndexMetadata.builder("leader").settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                    .numberOfShards(1).numberOfReplicas(0).build()
            return ClusterState.builder(ClusterName("leader")).metadata(Metadata.builder().put(index, false)).build()
        }
    }
}
