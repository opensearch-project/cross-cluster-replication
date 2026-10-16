/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.replication.util

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionType
import org.opensearch.action.admin.cluster.health.ClusterHealthAction
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.common.CheckedRunnable
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.identity.PluginSubject
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.client.NoOpNodeClient
import org.opensearch.threadpool.TestThreadPool
import org.opensearch.threadpool.ThreadPool
import java.security.Principal
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class PluginClientTests : OpenSearchTestCase() {

    private lateinit var threadPool: ThreadPool
    private lateinit var threadContext: ThreadContext
    private lateinit var responder: ExecutorService
    private lateinit var delegate: RecordingClient
    private lateinit var pluginClient: PluginClient

    override fun setUp() {
        super.setUp()
        threadPool = TestThreadPool(javaClass.simpleName)
        threadContext = threadPool.threadContext
        responder = Executors.newSingleThreadExecutor()
        delegate = RecordingClient()
        pluginClient = PluginClient(delegate)
    }

    override fun tearDown() {
        try {
            responder.shutdownNow()
            assertTrue("responder thread did not stop", responder.awaitTermination(10, TimeUnit.SECONDS))
        } finally {
            try {
                ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS)
            } finally {
                super.tearDown()
            }
        }
    }

    /**
     * Stands in for the plugin subject. Stashes and restores around its body as NoopPluginSubject and
     * SecurePluginSubject do, and marks the context so a test can tell the subject's apart from the caller's.
     */
    private inner class RecordingSubject : PluginSubject {
        override fun getPrincipal(): Principal = Principal { "plugin:replication" }

        override fun <E : Exception> runAs(runnable: CheckedRunnable<E>) {
            threadContext.stashContext().use {
                threadContext.putTransient(SUBJECT_TRANSIENT, "set-by-subject")
                runnable.run()
            }
        }
    }

    /**
     * Records the context the action ran in and keeps the listener, so each test decides when and how the
     * action completes. By default it does not complete, as for an asynchronous action.
     */
    private inner class RecordingClient : NoOpNodeClient(threadPool) {
        var subjectSeen: String? = null
        var callerSeen: String? = null
        var capturedListener: ActionListener<ClusterHealthResponse>? = null
        var behaviour: (ActionListener<ClusterHealthResponse>) -> Unit = {}

        @Suppress("UNCHECKED_CAST")
        override fun <Request : ActionRequest, Response : ActionResponse> doExecute(
            action: ActionType<Response>,
            request: Request,
            listener: ActionListener<Response>
        ) {
            subjectSeen = threadContext.getTransient(SUBJECT_TRANSIENT)
            callerSeen = threadContext.getTransient(CALLER_TRANSIENT)
            val typed = listener as ActionListener<ClusterHealthResponse>
            capturedListener = typed
            behaviour(typed)
        }
    }

    fun `test action runs inside the subject`() {
        threadContext.putTransient(CALLER_TRANSIENT, "set-by-caller")
        pluginClient.setSubject(RecordingSubject())

        execute(noopListener())

        assertThat(delegate.subjectSeen).describedAs("the action must run inside runAs").isEqualTo("set-by-subject")
        assertThat(delegate.callerSeen).describedAs("the caller's context must not reach the action").isNull()
    }

    fun `test listener sees the caller context and not the subject's on response`() {
        assertListenerSeesCallerContext { it.onResponse(healthResponse()) }
    }

    fun `test listener sees the caller context and not the subject's on failure`() {
        assertListenerSeesCallerContext { it.onFailure(RuntimeException("action failed")) }
    }

    fun `test responding thread's own context comes back after the listener returns`() {
        threadContext.putTransient(CALLER_TRANSIENT, "set-by-caller")
        pluginClient.setSubject(RecordingSubject())
        execute(noopListener())

        // Restoring the caller's context and leaving it there would strand it on a pooled thread for
        // whatever runs next.
        var responderAfter: String? = null
        var callerLeftBehind: String? = "not recorded"
        completeOnAnotherThread {
            it.onResponse(healthResponse())
            responderAfter = threadContext.getTransient(RESPONDER_TRANSIENT)
            callerLeftBehind = threadContext.getTransient(CALLER_TRANSIENT)
        }

        assertThat(responderAfter).describedAs("responding thread's own context must come back").isEqualTo("set-by-responder")
        assertThat(callerLeftBehind).describedAs("the caller's context must not be left on the responding thread").isNull()
    }

    // A raw listener also passes here; this guards a wrapper that drops response headers.
    fun `test listener sees a response header the action added`() {
        pluginClient.setSubject(RecordingSubject())
        var warnings: List<String>? = null
        execute(listener({ warnings = threadContext.responseHeaders["Warning"] }, {}))

        completeOnAnotherThread {
            threadContext.addResponseHeader("Warning", "deprecated")
            it.onResponse(healthResponse())
        }

        assertThat(warnings).containsExactly("deprecated")
    }

    fun `test a delegate that throws synchronously reaches the caller and not the listener`() {
        pluginClient.setSubject(RecordingSubject())
        val boom = RuntimeException("failed to find action")
        delegate.behaviour = { throw boom }

        val failures = mutableListOf<Exception>()
        assertThatThrownBy { execute(listener({}, { failures.add(it) })) }.isSameAs(boom)
        assertThat(failures).describedAs("a failure thrown before the listener exists must not also reach it").isEmpty()
    }

    fun `test a listener that throws is called once and its exception reaches the caller`() {
        pluginClient.setSubject(RecordingSubject())
        delegate.behaviour = { it.onResponse(healthResponse()) }

        val listenerBug = RuntimeException("listener bug")
        var calls = 0
        val throwing = object : ActionListener<ClusterHealthResponse> {
            override fun onResponse(response: ClusterHealthResponse) {
                calls++
                throw listenerBug
            }

            override fun onFailure(e: Exception) {
                calls++
            }
        }

        assertThatThrownBy { execute(throwing) }.isSameAs(listenerBug)
        assertThat(calls).describedAs("the listener must be told exactly once").isEqualTo(1)
    }

    fun `test the same subject twice is accepted and a different one is rejected`() {
        val first = RecordingSubject()
        pluginClient.setSubject(first)
        pluginClient.setSubject(first)

        assertThatThrownBy { pluginClient.setSubject(RecordingSubject()) }.isInstanceOf(IllegalStateException::class.java)
    }

    fun `test getRemoteClusterClient is refused`() {
        assertThatThrownBy { pluginClient.getRemoteClusterClient("leader") }
            .isInstanceOf(UnsupportedOperationException::class.java)
    }

    fun `test use before a subject is assigned is rejected`() {
        assertThatThrownBy { execute(noopListener()) }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("subject")
    }

    private fun assertListenerSeesCallerContext(completion: (ActionListener<ClusterHealthResponse>) -> Unit) {
        threadContext.putTransient(CALLER_TRANSIENT, "set-by-caller")
        pluginClient.setSubject(RecordingSubject())

        var callerSeen: String? = "listener did not run"
        var subjectSeen: String? = "listener did not run"
        val record = { _: Any ->
            callerSeen = threadContext.getTransient(CALLER_TRANSIENT)
            subjectSeen = threadContext.getTransient(SUBJECT_TRANSIENT)
        }
        execute(listener(record, record))

        // A response arrives on a pooled thread that never carried the caller's context, so the wrapper
        // around the listener is the only thing that can put it back.
        completeOnAnotherThread(completion)

        assertThat(callerSeen).isEqualTo("set-by-caller")
        assertThat(subjectSeen).describedAs("the subject's context must not leak into the listener").isNull()
    }

    /** Completes the captured action on a thread that never carried the caller's context. */
    private fun completeOnAnotherThread(completion: (ActionListener<ClusterHealthResponse>) -> Unit) {
        val listener = delegate.capturedListener ?: throw AssertionError("the action was never dispatched")
        responder.submit {
            threadContext.stashContext().use {
                threadContext.putTransient(RESPONDER_TRANSIENT, "set-by-responder")
                completion(listener)
            }
        }.get(10, TimeUnit.SECONDS)
    }

    private fun execute(listener: ActionListener<ClusterHealthResponse>) =
        pluginClient.execute(ClusterHealthAction.INSTANCE, ClusterHealthRequest(), listener)

    private fun listener(respond: (ClusterHealthResponse) -> Unit, fail: (Exception) -> Unit) =
        ActionListener.wrap<ClusterHealthResponse>(respond, fail)

    private fun noopListener() = listener({}, {})

    private fun healthResponse() = ClusterHealthResponse(ClusterName.DEFAULT.value(), emptyArray(), ClusterState.EMPTY_STATE)

    companion object {
        private const val CALLER_TRANSIENT = "test.caller"
        private const val SUBJECT_TRANSIENT = "test.subject"
        private const val RESPONDER_TRANSIENT = "test.responder"
    }
}
