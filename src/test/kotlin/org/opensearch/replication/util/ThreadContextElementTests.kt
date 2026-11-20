/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.replication.util

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.opensearch.core.action.ActionListener
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.TestThreadPool
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client
import org.opensearch.test.client.NoOpClient
import java.util.Collections
import java.util.concurrent.TimeUnit

/** Checks which context a wrapped client call runs in, and which context the coroutine has after it. */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class ThreadContextElementTests : OpenSearchTestCase() {

    companion object {
        private const val IDENTITY = "identity"
        private const val CALLER = "caller"
    }

    private lateinit var threadPool: ThreadPool
    private lateinit var client: Client

    override fun setUp() {
        super.setUp()
        threadPool = TestThreadPool(javaClass.simpleName)
        client = NoOpClient(threadPool)
    }

    override fun tearDown() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS)
        super.tearDown()
    }

    fun testTheCallerContextIsBackAfterEachWrappedCall() {
        val seen = Collections.synchronizedList(mutableListOf<String?>())
        threadPool.threadContext.putTransient(IDENTITY, CALLER)
        runBlocking {
            launch(threadPool.coroutineContext(ThreadPool.Names.GENERIC)) {
                seen.add(identity())
                seen.add("in call: " + client.suspending(::respondOnAnotherThread)("plain"))
                seen.add(identity())
                seen.add("in call: " + client.suspending(::respondOnAnotherThread, defaultContext = true)("default"))
                seen.add(identity())
                seen.add("in call: " + client.suspending(::respondOnAnotherThread, injectSecurityContext = true)("inject"))
                seen.add(identity())
            }.join()
        }
        assertEquals(
            listOf(CALLER, "in call: $CALLER", CALLER, "in call: null", CALLER, "in call: null", CALLER),
            seen
        )
    }

    private fun identity(): String? = threadPool.threadContext.getTransient(IDENTITY)

    /** Answers with the identity seen when the call is made, from another thread, so the coroutine really suspends. */
    private fun respondOnAnotherThread(label: String, listener: ActionListener<String?>) {
        val seenAtCall = identity()
        threadPool.generic().execute { listener.onResponse(seenAtCall) }
    }
}
