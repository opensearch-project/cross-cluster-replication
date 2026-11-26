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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.NonCancellable
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

    fun testAChildLaunchedAfterACallThatDoesNotSuspendSeesWhatWasPutBefore() {
        threadPool.threadContext.putTransient(IDENTITY, CALLER)
        val seen = runBlocking {
            withContext(threadPool.coroutineContext(ThreadPool.Names.GENERIC)) {
                threadPool.threadContext.putTransient(MARK, "m")
                client.suspending(::respondOnTheSameThread)("sync")
                val child = CompletableDeferred<String?>()
                launch { child.complete(threadPool.threadContext.getTransient(MARK)) }
                listOf(threadPool.threadContext.getTransient(MARK), child.await())
            }
        }
        assertEquals(listOf("m", "m"), seen)
    }

    fun testANestedWithContextSeesWhatWasPutBefore() {
        val seen = runBlocking {
            withContext(threadPool.coroutineContext(ThreadPool.Names.GENERIC)) {
                threadPool.threadContext.putTransient(MARK, "m")
                withContext(NonCancellable) { threadPool.threadContext.getTransient<String>(MARK) }
            }
        }
        assertEquals("m", seen)
    }

    fun testAnUnconfinedCoroutineDoesNotKeepTheContextACallWasSentIn() {
        threadPool.threadContext.putTransient(IDENTITY, CALLER)
        val out = CompletableDeferred<List<String?>>()
        GlobalScope.launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            client.suspending(::markAndRespondLikeTransport, defaultContext = true)("x")
            out.complete(listOf(identity(), threadPool.threadContext.getTransient(MARK)))
        }
        assertEquals(listOf(CALLER, null), runBlocking { out.await() })
        assertEquals("the launching thread", CALLER, identity())
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

    private fun respondOnTheSameThread(label: String, listener: ActionListener<String?>) {
        listener.onResponse(identity())
    }

    // Like the transport layer: the response is delivered in the context the request was sent in.
    private fun markAndRespondLikeTransport(label: String, listener: ActionListener<String?>) {
        threadPool.threadContext.putTransient(MARK, "sent")
        val sentIn = threadPool.threadContext.newRestorableContext(false)
        threadPool.generic().execute { sentIn.get().use { listener.onResponse(null) } }
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
