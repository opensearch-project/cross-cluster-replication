/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.replication.util

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
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
        private const val MARK = "mark"
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

    fun testCoroutinesInOneScopeDoNotSeeEachOthersContext() {
        threadPool.threadContext.putTransient(IDENTITY, CALLER)
        val scope = CoroutineScope(threadPool.coroutineContext(ThreadPool.Names.GENERIC))
        val aSuspended = CompletableDeferred<Unit>()
        val bRead = CompletableDeferred<Unit>()
        var seenByA: String? = null
        var seenByB: String? = "unset"
        var sharedByB: String? = "unset"

        runBlocking {
            val a = scope.launch {
                threadPool.threadContext.putTransient(MARK, "A")
                aSuspended.complete(Unit)
                bRead.await()
                seenByA = threadPool.threadContext.getTransient(MARK)
            }
            aSuspended.await()
            val b = scope.launch {
                sharedByB = identity()
                seenByB = threadPool.threadContext.getTransient(MARK)
                bRead.complete(Unit)
            }
            joinAll(a, b)
        }
        scope.cancel()

        assertEquals("B starts from the scope's context", CALLER, sharedByB)
        assertNull("B does not see what A put in its context", seenByB)
        assertEquals("A keeps what it put in its context", "A", seenByA)
    }

    fun testTheContextIsBackAfterAFailedCall() {
        threadPool.threadContext.putTransient(IDENTITY, CALLER)
        val seen = runBlocking {
            withContext(threadPool.coroutineContext(ThreadPool.Names.GENERIC)) {
                expectThrows(IllegalStateException::class.java) {
                    runBlocking { client.suspending(::failOnAnotherThread)("fail") }
                }
                val inner = try {
                    client.suspending(::failOnAnotherThread)("fail")
                    "no failure"
                } catch (e: IllegalStateException) {
                    identity()
                }
                inner
            }
        }
        assertEquals(CALLER, seen)
    }

    fun testTheContextIsBackWhenACallIsCancelled() {
        threadPool.threadContext.putTransient(IDENTITY, CALLER)
        var seenInFinally: String? = "unset"
        runBlocking {
            val job = launch(threadPool.coroutineContext(ThreadPool.Names.GENERIC)) {
                try {
                    client.suspending(::neverRespond)("hang")
                } finally {
                    seenInFinally = identity()
                }
            }
            delay(100)
            job.cancelAndJoin()
        }
        assertEquals(CALLER, seenInFinally)
    }

    fun testAValuePutBetweenCallsIsKept() {
        threadPool.threadContext.putTransient(IDENTITY, CALLER)
        val seen = runBlocking {
            withContext(threadPool.coroutineContext(ThreadPool.Names.GENERIC)) {
                client.suspending(::respondOnAnotherThread)("first")
                threadPool.threadContext.putTransient(MARK, "between")
                val inCall = client.suspending(::respondMarkOnAnotherThread)("second")
                listOf(inCall, threadPool.threadContext.getTransient<String>(MARK))
            }
        }
        assertEquals(listOf("between", "between"), seen)
    }

    fun testInDefaultContextRunsWithoutTheCaller() {
        threadPool.threadContext.putTransient(IDENTITY, CALLER)
        val seen = runBlocking {
            withContext(threadPool.coroutineContext(ThreadPool.Names.GENERIC)) {
                val inside = threadPool.inDefaultContext { client.suspending(::respondOnAnotherThread)("default") }
                listOf(inside, identity())
            }
        }
        assertEquals(listOf(null, CALLER), seen)
    }

    fun testACallerThreadWithoutAnElementKeepsItsContext() {
        threadPool.threadContext.putTransient(IDENTITY, CALLER)
        val inCall = runBlocking { client.suspending(::respondOnAnotherThread)("plain") }
        assertEquals(CALLER, inCall)
        assertEquals("the calling thread after runBlocking", CALLER, identity())
    }

    private fun identity(): String? = threadPool.threadContext.getTransient(IDENTITY)

    /** Answers with the identity seen when the call is made, from another thread, so the coroutine really suspends. */
    private fun failOnAnotherThread(label: String, listener: ActionListener<String?>) {
        threadPool.generic().execute { listener.onFailure(IllegalStateException(label)) }
    }

    @Suppress("UNUSED_PARAMETER")
    private fun neverRespond(label: String, listener: ActionListener<String?>) {
    }

    private fun respondMarkOnAnotherThread(label: String, listener: ActionListener<String?>) {
        val seenAtCall = threadPool.threadContext.getTransient<String>(MARK)
        threadPool.generic().execute { listener.onResponse(seenAtCall) }
    }

    private fun respondOnAnotherThread(label: String, listener: ActionListener<String?>) {
        val seenAtCall = identity()
        threadPool.generic().execute { listener.onResponse(seenAtCall) }
    }
}
