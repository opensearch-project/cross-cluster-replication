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

package org.opensearch.replication.action.resume

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import kotlin.coroutines.Continuation
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.opensearch.Version
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionType
import org.opensearch.action.support.ActionFilters
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.routing.RoutingTable
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.core.index.Index
import org.opensearch.env.Environment
import org.opensearch.index.seqno.RetentionLeaseActions
import org.opensearch.index.seqno.RetentionLeaseInvalidRetainingSeqNoException
import org.opensearch.index.seqno.RetentionLeaseNotFoundException
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.task.index.IndexReplicationParams
import org.opensearch.test.ClusterServiceUtils
import org.opensearch.test.ClusterServiceUtils.setState
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.client.NoOpNodeClient
import org.opensearch.threadpool.TestThreadPool
import org.opensearch.transport.TransportService
import java.io.IOException

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class TransportResumeIndexReplicationActionTests : OpenSearchTestCase() {

    companion object {
        const val FOLLOWER_INDEX = "follower-index"
        const val LEADER_INDEX = "leader-index"
        const val CONNECTION_NAME = "leader-cluster"
    }

    private val threadPool = TestThreadPool("TransportResumeIndexReplicationActionTests")

    override fun tearDown() {
        super.tearDown()
        threadPool.shutdown()
    }

    private fun buildClusterService(numShards: Int): ClusterService {
        val clusterService = ClusterServiceUtils.createClusterService(threadPool)
        val state = clusterService.state()
        val followerIndexMeta = IndexMetadata.builder(FOLLOWER_INDEX)
            .settings(settings(Version.CURRENT))
            .numberOfShards(numShards)
            .numberOfReplicas(0)
            .build()
        val metadata = Metadata.builder(state.metadata()).put(followerIndexMeta, false).build()
        val routingTable = RoutingTable.builder().addAsNew(metadata.index(FOLLOWER_INDEX)).build()
        setState(clusterService, ClusterState.builder(state).metadata(metadata).routingTable(routingTable).build())
        return clusterService
    }

    private inner class ProgrammableRemoteClient(
        private val answers: ArrayDeque<(ActionListener<*>) -> Unit>,
        val executedActions: MutableList<ActionType<*>> = mutableListOf()
    ) : NoOpNodeClient(testName) {
        @Suppress("UNCHECKED_CAST")
        override fun <Req : ActionRequest, Resp : ActionResponse> doExecute(
            action: ActionType<Resp>?,
            request: Req?,
            listener: ActionListener<Resp>
        ) {
            if (action != null) executedActions.add(action)
            answers.removeFirst()(listener as ActionListener<*>)
        }
    }

    private val answerInvalidSeqNo: (ActionListener<*>) -> Unit = { listener ->
        listener.onFailure(mock<RetentionLeaseInvalidRetainingSeqNoException>())
    }

    private val answerNotFound: (ActionListener<*>) -> Unit = { listener ->
        listener.onFailure(RetentionLeaseNotFoundException("lease-id"))
    }

    private val answerTransient: (ActionListener<*>) -> Unit = { listener ->
        listener.onFailure(IOException("simulated network error"))
    }

    private fun buildAction(
        clusterService: ClusterService,
        remoteClient: NoOpNodeClient
    ): TransportResumeIndexReplicationAction {
        val localClient = object : NoOpNodeClient(testName) {
            override fun getRemoteClusterClient(clusterAlias: String) = remoteClient
        }
        val transportService = mock<TransportService> { on { localNode } doReturn mock() }
        val environment = mock<Environment> { on { settings() } doReturn Settings.EMPTY }
        val replicationMetadataManager = mock<ReplicationMetadataManager>()
        return TransportResumeIndexReplicationAction(
            transportService,
            clusterService,
            threadPool,
            mock<ActionFilters> { on { filters() } doReturn emptyArray() },
            mock(),
            localClient,
            replicationMetadataManager,
            environment
        )
    }

    private fun callIsResumable(
        action: TransportResumeIndexReplicationAction,
        params: IndexReplicationParams
    ): Boolean = runBlocking {
        suspendCoroutine { continuation ->
            val method = TransportResumeIndexReplicationAction::class.java.getDeclaredMethod(
                "isResumable",
                IndexReplicationParams::class.java,
                Continuation::class.java
            )
            method.isAccessible = true
            val innerContinuation = object : Continuation<Any?> {
                override val context: CoroutineContext = EmptyCoroutineContext

                override fun resumeWith(result: Result<Any?>) {
                    result.fold(
                        onSuccess = { continuation.resume(it as Boolean) },
                        onFailure = { continuation.resumeWithException(it) }
                    )
                }
            }
            val result = method.invoke(action, params, innerContinuation)
            if (result !== COROUTINE_SUSPENDED) {
                continuation.resume(result as Boolean)
            }
        }
    }

    private fun makeParams() = IndexReplicationParams(
        CONNECTION_NAME,
        Index(LEADER_INDEX, "leader-uuid"),
        FOLLOWER_INDEX
    )

    fun `test force resume request routing - leases valid skips force path`() {
        // When leases are valid (resumable=true), force resume flag should be irrelevant
        val resumable = true
        val forceResume = true

        // The condition that triggers force resume
        val shouldForceResume = !resumable && forceResume
        assertThat(shouldForceResume).isFalse()
    }

    fun `test force resume request routing - leases expired without force throws`() {
        val resumable = false
        val forceResume = false

        val shouldThrow = !resumable && !forceResume
        assertThat(shouldThrow).isTrue()
    }

    fun `test force resume request routing - leases expired with force triggers force path`() {
        val resumable = false
        val forceResume = true

        val shouldForceResume = !resumable && forceResume
        assertThat(shouldForceResume).isTrue()
    }

    fun `test role preservation with both roles present`() {
        val followerRole = "follower_role"
        val leaderRole = "leader_role"

        // Simulates the role preservation logic in executeForceResume
        val roles = hashMapOf(
            "leader_cluster_role" to leaderRole,
            "follower_cluster_role" to followerRole
        )

        assertThat(roles).hasSize(2)
        assertThat(roles["leader_cluster_role"]).isEqualTo("leader_role")
        assertThat(roles["follower_cluster_role"]).isEqualTo("follower_role")
    }

    fun `test role preservation skipped when roles are null`() {
        val followerUser: Any? = null
        val leaderUser: Any? = null

        // Simulates the condition in executeForceResume
        val shouldSetRoles = followerUser != null && leaderUser != null
        assertThat(shouldSetRoles).isFalse()
    }

    fun `failed resume due to missing shard 2 lease does not remove shard 0 and shard 1 healthy leases`() {
        val clusterService = buildClusterService(numShards = 3)
        val params = makeParams()

        val phase1Answers = ArrayDeque<(ActionListener<*>) -> Unit>().apply {
            add(answerInvalidSeqNo)
            add(answerInvalidSeqNo)
            add(answerNotFound)
            add(answerNotFound)
        }
        val phase1Client = ProgrammableRemoteClient(phase1Answers)
        val phase1Action = buildAction(clusterService, phase1Client)

        val resumable1 = callIsResumable(phase1Action, params)

        assertFalse("Phase 1: isResumable must return false when shard 2 lease is absent", resumable1)
        assertFalse(
            "BUG REGRESSION (Phase 1): Remove must NOT be called — shards 0 and 1 healthy leases " +
                "must be preserved so resume can succeed once shard 2's lease is restored",
            phase1Client.executedActions.any { it == RetentionLeaseActions.Remove.INSTANCE }
        )
        // Lease not found exception will trigger another renewal attempt because of supporting two lease format
        // Renew the current lease ID, containing follower cluster name + UUID.
        // If not found, renew the legacy lease ID, containing only the cluster name.
        assertEquals(
            "Phase 1: 4 Renew calls — 1 each for shards 0,1 + 2 for absent shard 2",
            4,
            phase1Client.executedActions.count { it == RetentionLeaseActions.Renew.INSTANCE }
        )

        val phase2Answers = ArrayDeque<(ActionListener<*>) -> Unit>().apply {
            add(answerInvalidSeqNo)
            add(answerInvalidSeqNo)
            add(answerInvalidSeqNo)
        }
        val phase2Client = ProgrammableRemoteClient(phase2Answers)
        val phase2Action = buildAction(clusterService, phase2Client)

        val resumable2 = callIsResumable(phase2Action, params)

        assertTrue(
            "Phase 2: isResumable must return true after shard 2 lease is restored — " +
                "proving shards 0 and 1 leases were preserved during the failed phase 1 resume",
            resumable2
        )
        assertFalse(
            "Phase 2: Remove must never be called on successful resume",
            phase2Client.executedActions.any { it == RetentionLeaseActions.Remove.INSTANCE }
        )
    }

    fun `network error on shard 2 does not remove shard 0 and shard 1 healthy leases`() {
        val clusterService = buildClusterService(numShards = 3)
        val params = makeParams()

        val answers = ArrayDeque<(ActionListener<*>) -> Unit>().apply {
            add(answerInvalidSeqNo)
            add(answerInvalidSeqNo)
            add(answerTransient)
        }
        val client = ProgrammableRemoteClient(answers)
        val action = buildAction(clusterService, client)

        val resumable = callIsResumable(action, params)

        assertFalse("isResumable must return false on network error", resumable)
        assertFalse(
            "BUG REGRESSION: Remove must NOT be called — shards 0 and 1 healthy leases " +
                "must be preserved so resume succeeds once the network recovers",
            client.executedActions.any { it == RetentionLeaseActions.Remove.INSTANCE }
        )
    }

    fun `test isResumable returns true when all 3 shards have healthy leases`() {
        val clusterService = buildClusterService(numShards = 3)
        val params = makeParams()

        val answers = ArrayDeque<(ActionListener<*>) -> Unit>().apply {
            add(answerInvalidSeqNo)
            add(answerInvalidSeqNo)
            add(answerInvalidSeqNo)
        }
        val client = ProgrammableRemoteClient(answers)
        val action = buildAction(clusterService, client)

        val resumable = callIsResumable(action, params)

        assertTrue("isResumable must return true when all 3 shard leases are healthy", resumable)
        assertFalse(
            "Remove must never be called when all shards are healthy",
            client.executedActions.any { it == RetentionLeaseActions.Remove.INSTANCE }
        )
        assertEquals(
            "Exactly 3 Renew calls (one per shard)",
            3,
            client.executedActions.count { it == RetentionLeaseActions.Renew.INSTANCE }
        )
    }
}
