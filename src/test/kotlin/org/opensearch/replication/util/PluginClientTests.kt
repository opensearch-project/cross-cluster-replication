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
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionType
import org.opensearch.action.admin.cluster.health.ClusterHealthAction
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse
import org.opensearch.cluster.ClusterName
import org.opensearch.common.CheckedRunnable
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.identity.Subject
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.client.NoOpNodeClient
import org.opensearch.threadpool.TestThreadPool
import org.opensearch.threadpool.ThreadPool
import java.security.Principal
import java.util.concurrent.TimeUnit

class PluginClientTests : OpenSearchTestCase() {

    private lateinit var threadPool: ThreadPool

    override fun setUp() {
        super.setUp()
        threadPool = TestThreadPool(javaClass.simpleName)
    }

    override fun tearDown() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS)
        super.tearDown()
    }

    /**
     * A [Subject] that records the thread context it was handed and marks the context while the
     * action runs, standing in for the security plugin switching to the plugin user.
     */
    private inner class RecordingSubject(private val failWith: Exception? = null) : Subject {
        var callerHeaderSeenInsideRunAs: String? = null

        override fun getPrincipal(): Principal = Principal { "plugin:replication" }

        override fun <E : Exception> runAs(runnable: CheckedRunnable<E>) {
            callerHeaderSeenInsideRunAs = threadPool.threadContext.getHeader(CALLER_HEADER)
            if (failWith != null) {
                throw failWith
            }
            val stashed = threadPool.threadContext.stashContext()
            try {
                threadPool.threadContext.putHeader(SUBJECT_HEADER, "yes")
                runnable.run()
            } finally {
                stashed.close()
            }
        }
    }

    /** Records the thread context that the wrapped action was actually dispatched under. */
    private inner class RecordingClient : NoOpNodeClient(threadPool) {
        var subjectHeaderAtDispatch: String? = null
        var callerHeaderAtDispatch: String? = null
        var capturedListener: ActionListener<*>? = null

        override fun <Request : ActionRequest, Response : ActionResponse> doExecute(
            action: ActionType<Response>,
            request: Request,
            listener: ActionListener<Response>
        ) {
            subjectHeaderAtDispatch = threadPool.threadContext.getHeader(SUBJECT_HEADER)
            callerHeaderAtDispatch = threadPool.threadContext.getHeader(CALLER_HEADER)
            capturedListener = listener
        }
    }

    fun `test action is dispatched under the subject and sees the caller context on entry`() {
        val delegate = RecordingClient()
        val subject = RecordingSubject()
        val pluginClient = PluginClient(delegate)
        pluginClient.setSubject(subject)

        threadPool.threadContext.putHeader(CALLER_HEADER, "caller")
        pluginClient.execute(ClusterHealthAction.INSTANCE, ClusterHealthRequest(), noopListener())

        // The client must not clear the context itself: runAs is what performs the switch, so the
        // caller's context has to still be in place when runAs is entered.
        assertThat(subject.callerHeaderSeenInsideRunAs).isEqualTo("caller")
        assertThat(delegate.subjectHeaderAtDispatch).isEqualTo("yes")
        assertThat(delegate.callerHeaderAtDispatch).isNull()
    }

    fun `test caller context is restored on the thread the listener completes on`() {
        val delegate = RecordingClient()
        val pluginClient = PluginClient(delegate)
        pluginClient.setSubject(RecordingSubject())

        var callerHeaderInsideListener: String? = null
        threadPool.threadContext.putHeader(CALLER_HEADER, "caller")
        pluginClient.execute(ClusterHealthAction.INSTANCE, ClusterHealthRequest(),
            object : ActionListener<ClusterHealthResponse> {
                override fun onResponse(response: ClusterHealthResponse) {
                    callerHeaderInsideListener = threadPool.threadContext.getHeader(CALLER_HEADER)
                }

                override fun onFailure(e: Exception) = fail("expected a response")
            })

        // Completed from a thread that never carried the caller's context, which is what a transport
        // response thread looks like. Only a restore driven by the listener can put it back there.
        @Suppress("UNCHECKED_CAST")
        val listener = delegate.capturedListener as ActionListener<ClusterHealthResponse>
        val responder = Thread { listener.onResponse(healthResponse()) }
        responder.start()
        responder.join()

        assertThat(callerHeaderInsideListener).isEqualTo("caller")
    }

    fun `test a synchronous failure is reported through the listener`() {
        val failure = IllegalArgumentException("no subject permissions")
        val pluginClient = PluginClient(RecordingClient())
        pluginClient.setSubject(RecordingSubject(failWith = failure))

        var reported: Exception? = null
        pluginClient.execute(ClusterHealthAction.INSTANCE, ClusterHealthRequest(),
            object : ActionListener<ClusterHealthResponse> {
                override fun onResponse(response: ClusterHealthResponse) = fail("expected a failure")
                override fun onFailure(e: Exception) {
                    reported = e
                }
            })

        // Throwing instead would leave an asynchronous caller waiting for a listener that never fires.
        assertThat(reported).isSameAs(failure)
    }

    private fun noopListener() = object : ActionListener<ClusterHealthResponse> {
        override fun onResponse(response: ClusterHealthResponse) {}
        override fun onFailure(e: Exception) {}
    }

    private fun healthResponse() = ClusterHealthResponse(ClusterName.DEFAULT.value(), emptyArray(),
        org.opensearch.cluster.ClusterState.EMPTY_STATE)

    companion object {
        private const val CALLER_HEADER = "test.caller"
        private const val SUBJECT_HEADER = "test.subject"
    }
}
