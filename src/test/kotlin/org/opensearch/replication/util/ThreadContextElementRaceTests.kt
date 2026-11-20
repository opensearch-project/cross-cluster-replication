/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.replication.util

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import kotlinx.coroutines.ThreadContextElement
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.mockito.Mockito
import org.opensearch.core.action.ActionListener
import org.opensearch.index.shard.GlobalCheckpointListeners
import org.opensearch.index.shard.IndexShard
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.client.NoOpClient
import org.opensearch.threadpool.TestThreadPool
import org.opensearch.threadpool.ThreadPool
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

/** Checks the thread context element when a response resumes the coroutine before the suspending thread has unwound. */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class ThreadContextElementRaceTests : OpenSearchTestCase() {

    private lateinit var threadPool: ThreadPool

    override fun setUp() {
        super.setUp()
        threadPool = TestThreadPool(javaClass.simpleName)
    }

    override fun tearDown() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS)
        super.tearDown()
    }

    fun testAResumeThatOvertakesTheUnwindDoesNotLeaveTheElementMarkedOnTheOldThread() {
        val tc = threadPool.threadContext
        val t1 = Executors.newSingleThreadExecutor()
        val t2 = Executors.newSingleThreadExecutor()
        fun <T> on(executor: ExecutorService, block: () -> T): T = executor.submit(block).get()
        try {
            on(t1) { tc.putTransient("identity", "caller") }
            val element = on(t1) { threadPool.coroutineContext() as OpenSearchThreadContextElement }
            on(t1) { tc.stashContext(); tc.putTransient("identity", "t1-own") }
            on(t2) { tc.putTransient("identity", "t2-own") }
            val ctx = EmptyCoroutineContext
            val s0 = on(t1) { element.updateThreadContext(ctx) }   // resume on T1
            val sN = on(t1) { element.updateThreadContext(ctx) }   // nested withContext entry
            on(t1) { element.restoreThreadContext(ctx, sN) }       // nested exit at the suspension
            val s1 = on(t2) { element.updateThreadContext(ctx) }   // the response resumes the inner block on T2 first
            on(t2) { element.restoreThreadContext(ctx, s1) }       // inner block done
            val s2 = on(t2) { element.updateThreadContext(ctx) }   // outer coroutine resumes on T2
            on(t1) { element.restoreThreadContext(ctx, s0) }       // T1 finally unwinds
            on(t2) { element.restoreThreadContext(ctx, s2) }       // outer coroutine suspends on T2
            val s3 = on(t1) { element.updateThreadContext(ctx) }   // the next resume lands on T1
            val seen = on(t1) { tc.getTransient<String>("identity") }
            on(t1) { element.restoreThreadContext(ctx, s3) }
            assertEquals("caller", seen)
            assertEquals("T1 has its own context back", "t1-own", on(t1) { tc.getTransient<String>("identity") })
        } finally {
            t1.shutdown()
            t2.shutdown()
        }
    }

    fun testALateUnwindDoesNotOverwriteWhatTheResumedCoroutineSaved() {
        val tc = threadPool.threadContext
        val t1 = Executors.newSingleThreadExecutor()
        val t2 = Executors.newSingleThreadExecutor()
        fun <T> on(executor: ExecutorService, block: () -> T): T = executor.submit(block).get()
        try {
            on(t1) { tc.putTransient("identity", "caller") }
            val element = on(t1) { threadPool.coroutineContext() as OpenSearchThreadContextElement }
            on(t1) { tc.stashContext() }
            val ctx = EmptyCoroutineContext
            val s0 = on(t1) { element.updateThreadContext(ctx) }   // resume on T1
            on(t1) { element.saveCurrent() }                       // a wrapped call saves before it sends
            val s1 = on(t2) { element.updateThreadContext(ctx) }   // the response resumes the coroutine on T2
            on(t2) { tc.putTransient("mark", "after") }            // the coroutine goes on and puts a value
            on(t2) { element.restoreThreadContext(ctx, s1) }       // and suspends again
            on(t1) { element.restoreThreadContext(ctx, s0) }       // T1 unwinds only now
            val s2 = on(t1) { element.updateThreadContext(ctx) }
            val seen = on(t1) { tc.getTransient<String>("mark") }
            on(t1) { element.restoreThreadContext(ctx, s2) }
            assertEquals("after", seen)
        } finally {
            t1.shutdown()
            t2.shutdown()
        }
    }

    fun testAValuePutBeforeASavedSuspensionSurvivesAResumeThatOvertookTheUnwind() {
        val tc = threadPool.threadContext
        val t1 = Executors.newSingleThreadExecutor()
        val t2 = Executors.newSingleThreadExecutor()
        fun <T> on(executor: ExecutorService, block: () -> T): T = executor.submit(block).get()
        try {
            val element = on(t1) { threadPool.coroutineContext() as OpenSearchThreadContextElement }
            val ctx = EmptyCoroutineContext
            val s0 = on(t1) { element.updateThreadContext(ctx) }
            on(t1) { tc.putTransient("mark", "m") }                // coroutine code before a suspension point
            on(t1) { element.saveCurrent() }                       // what every suspend helper does first
            val s1 = on(t2) { element.updateThreadContext(ctx) }   // the resume overtakes the unwind
            val seen = on(t2) { tc.getTransient<String>("mark") }
            on(t1) { element.restoreThreadContext(ctx, s0) }
            on(t2) { element.restoreThreadContext(ctx, s1) }
            assertEquals("m", seen)
        } finally {
            t1.shutdown()
            t2.shutdown()
        }
    }

    fun testAValuePutBeforeAHelperSuspensionSurvivesAResumeThatOvertookTheUnwind() {
        val shard = Mockito.mock(IndexShard::class.java)
        val responder = Executors.newSingleThreadExecutor()
        Mockito.doAnswer { invocation ->
            val listener = invocation.getArgument<GlobalCheckpointListeners.GlobalCheckpointListener>(1)
            responder.execute { listener.accept(5, null) }
            null
        }.`when`(shard).addGlobalCheckpointListener(Mockito.anyLong(), Mockito.any(), Mockito.any())
        try {
            val seen = runBlocking {
                withContext(threadPool.coroutineContext(ThreadPool.Names.GENERIC) + SlowFirstUnwind()) {
                    threadPool.threadContext.putTransient("mark", "m")
                    shard.waitForGlobalCheckpoint(5, null)
                    threadPool.threadContext.getTransient<String>("mark")
                }
            }
            assertEquals("m", seen)
        } finally {
            responder.shutdown()
        }
    }

    /** Slows the first suspensions down, so the first responses resume the coroutine before the old thread unwinds. */
    private class SlowFirstUnwind : ThreadContextElement<Unit>, CoroutineContext.Key<SlowFirstUnwind> {
        val restores = AtomicInteger()
        override val key: CoroutineContext.Key<*> get() = this
        override fun updateThreadContext(context: CoroutineContext) {}
        override fun restoreThreadContext(context: CoroutineContext, oldState: Unit) {
            if (restores.incrementAndGet() <= 2) Thread.sleep(50)
        }
    }

    fun testTheCallerContextSurvivesManyCallsAfterAResumeOvertookTheUnwind() {
        val client = NoOpClient(threadPool)
        threadPool.threadContext.putTransient("identity", "caller")
        val calls = AtomicInteger()
        val responder = Executors.newSingleThreadExecutor()
        try {
            val seen = runBlocking {
                withContext(threadPool.coroutineContext(ThreadPool.Names.GENERIC) + SlowFirstUnwind()) {
                    (1..400).map {
                        client.suspending({ _: String, listener: ActionListener<String?> ->
                            val n = calls.incrementAndGet()
                            val sentIn = threadPool.threadContext.newRestorableContext(false)
                            responder.execute {
                                if (n > 1) Thread.sleep(2)
                                sentIn.get().use { listener.onResponse(null) }
                            }
                        }, defaultContext = true)("x")
                        threadPool.threadContext.getTransient<String>("identity")
                    }
                }
            }
            val lost = seen.withIndex().filter { it.value != "caller" }
            assertTrue("calls after which the coroutine lost its context: ${lost.take(5)} (${lost.size} of ${seen.size})", lost.isEmpty())
        } finally {
            responder.shutdown()
        }
    }

    fun testAValuePutBeforeACallSurvivesAResumeThatOvertookTheUnwind() {
        val client = NoOpClient(threadPool)
        threadPool.threadContext.putTransient("identity", "caller")
        val responder = Executors.newSingleThreadExecutor()
        try {
            val seen = runBlocking {
                withContext(threadPool.coroutineContext(ThreadPool.Names.GENERIC) + SlowFirstUnwind()) {
                    threadPool.threadContext.putTransient("mark", "m")
                    client.suspending({ _: String, listener: ActionListener<String?> ->
                        val sentIn = threadPool.threadContext.newRestorableContext(false)
                        responder.execute { sentIn.get().use { listener.onResponse(null) } }
                    }, defaultContext = true)("x")
                    threadPool.threadContext.getTransient<String>("mark")
                }
            }
            assertEquals("m", seen)
        } finally {
            responder.shutdown()
        }
    }
}
