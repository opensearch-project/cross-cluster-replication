/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.elasticsearch.replication.util

import com.amazon.elasticsearch.replication.metadata.store.ReplicationMetadata
import kotlinx.coroutines.*
import org.elasticsearch.ElasticsearchTimeoutException
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.action.ActionType
import org.elasticsearch.action.support.master.AcknowledgedRequest
import org.elasticsearch.action.support.master.MasterNodeRequest
import org.elasticsearch.client.Client
import org.elasticsearch.client.ElasticsearchClient
import org.elasticsearch.cluster.*
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.Priority
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.util.concurrent.ThreadContext
import org.elasticsearch.index.shard.GlobalCheckpointListeners
import org.elasticsearch.index.shard.IndexShard
import org.elasticsearch.persistent.PersistentTaskParams
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask
import org.elasticsearch.persistent.PersistentTasksService
import org.elasticsearch.threadpool.ThreadPool
import java.util.concurrent.TimeoutException
import kotlin.coroutines.*

/**
 * Converts methods that take an ActionListener callback into a suspending function. Any method in [Client] that takes
 * an [ActionListener] callback can be translated to a suspending function as follows:
 *
 *     client.get(GetRequest("index", "id"), object: ActionListener<GetResponse> {
 *          override fun onResponse(resp: GetResponse) { ... }
 *          override fun onFailure(e: Exception) { ... }
 *          }
 *     )

 *     // becomes the much more readable (and chain-able)...
 *     val response = suspending(client::get)(GetRequest("index", "id")) // throws exception on failure that can be caught
 *
 * @param fn - a block of code that is passed an [ActionListener] that should be passed to the ES client API.
 */
suspend fun <Req, Resp> Client.suspending(fn: (Req, ActionListener<Resp>) -> Unit,
                                  injectSecurityContext: Boolean = false,
                                  defaultContext: Boolean = false): suspend (Req) -> Resp {
    return { req: Req ->
        withContext(this.threadPool().coroutineContext(null, "default", injectSecurityContext, defaultContext)) {
            suspendCancellableCoroutine<Resp> { cont -> fn(req, CoroutineActionListener(cont)) }
        }
    }
}

suspend fun <Req, Resp> Client.suspending(replicationMetadata: ReplicationMetadata,
                                  fn: (req: Req, ActionListener<Resp>) -> Unit,
                                  defaultContext: Boolean = false): suspend (Req) -> Resp {
    return { req: Req ->
        withContext(this.threadPool().coroutineContext(replicationMetadata, "default", true, defaultContext)) {
            suspendCancellableCoroutine<Resp> { cont -> fn(req, CoroutineActionListener(cont)) }
        }
    }
}

suspend fun <Req: ActionRequest, Resp: ActionResponse>
        ElasticsearchClient.suspendExecute(action: ActionType<Resp>, req: Req,
                                           injectSecurityContext: Boolean = false,
                                           defaultContext: Boolean = false) : Resp {
    return withContext(this.threadPool().coroutineContext(null, action.name(), injectSecurityContext, defaultContext)) {
        suspendCancellableCoroutine<Resp> { cont -> execute(action, req, CoroutineActionListener(cont)) }
    }
}

suspend fun <Req: ActionRequest, Resp: ActionResponse>
        ElasticsearchClient.suspendExecute(replicationMetadata: ReplicationMetadata,
                                           action: ActionType<Resp>, req: Req, defaultContext: Boolean = false) : Resp {
    return withContext(this.threadPool().coroutineContext(replicationMetadata, action.name(), true, defaultContext)) {
        suspendCancellableCoroutine<Resp> { cont ->
            execute(action, req, CoroutineActionListener(cont))
        }
    }
}

suspend fun <Req: ActionRequest, Resp: ActionResponse>
        ElasticsearchClient.suspendExecute(replicationMetadata: ReplicationMetadata?,
                                           action: ActionType<Resp>, req: Req, injectSecurityContext: Boolean = false, defaultContext: Boolean = false) : Resp {
    return if(replicationMetadata != null) {
        suspendExecute(replicationMetadata, action, req, defaultContext = defaultContext)
    } else {
        suspendExecute(action, req, injectSecurityContext = injectSecurityContext, defaultContext = defaultContext)
    }
}

suspend fun IndexShard.waitForGlobalCheckpoint(waitingForGlobalCheckpoint: Long, timeout: TimeValue?) : Long {
    return suspendCancellableCoroutine {  cont ->
        val listener = object : GlobalCheckpointListeners.GlobalCheckpointListener {

            // The calling coroutine context should be configured with an explicit executor so this choice shouldn't matter
            override fun executor() = Dispatchers.Default.asExecutor()

            override fun accept(gcp: Long, e: Exception?) {
                when {
                    e is TimeoutException -> cont.resumeWithException(ElasticsearchTimeoutException(e.message))
                    e != null -> cont.resumeWithException(e)
                    else -> cont.resume(gcp)
                }
            }

        }
        addGlobalCheckpointListener(waitingForGlobalCheckpoint, listener, timeout)
    }
}

suspend fun ClusterStateObserver.waitForNextChange(reason: String, predicate: (ClusterState) -> Boolean = { true }) {
    return suspendCancellableCoroutine { cont ->
        waitForNextChange(object : ClusterStateObserver.Listener {
            override fun onClusterServiceClose() {
                cont.cancel()
            }

            override fun onNewClusterState(state: ClusterState?) {
                cont.resume(Unit)
            }

            override fun onTimeout(timeout: TimeValue?) {
                cont.resumeWithException(ElasticsearchTimeoutException("timed out waiting for $reason"))
            }
        }, predicate)
    }
}

suspend fun <T> ClusterService.waitForClusterStateUpdate(source: String,
                                                         updateTaskFactory: (ActionListener<T>) ->
                                                         AckedClusterStateUpdateTask<T>) : T =
    suspendCoroutine { cont -> submitStateUpdateTask(source, updateTaskFactory(CoroutineActionListener(cont))) }

suspend fun <T : PersistentTaskParams>
    PersistentTasksService.startTask(taskId: String, taskName: String, params : T): PersistentTask<T> {
    return suspendCoroutine { cont -> this.sendStartRequest(taskId, taskName, params, CoroutineActionListener(cont)) }
}

suspend fun PersistentTasksService.removeTask(taskId: String): PersistentTask<*> {
    return suspendCoroutine { cont -> this.sendRemoveRequest(taskId, CoroutineActionListener(cont)) }
}

suspend fun PersistentTasksService.waitForTaskCondition(taskId: String, timeout: TimeValue,
                                                        condition: (PersistentTask<*>) -> Boolean) : PersistentTask<*> {
    return suspendCancellableCoroutine { cont ->
        val listener = object : PersistentTasksService.WaitForPersistentTaskListener<PersistentTaskParams> {
            override fun onResponse(response: PersistentTask<PersistentTaskParams>) = cont.resume(response)
            override fun onFailure(e: Exception) = cont.resumeWithException(e)
        }
        waitForPersistentTaskCondition(taskId, { p -> condition(p) }, timeout, listener)
    }
}

class CoroutineActionListener<T>(private val continuation: Continuation<T>) : ActionListener<T> {
    override fun onResponse(result: T) = continuation.resume(result)
    override fun onFailure(e: Exception) = continuation.resumeWithException(ExceptionsHelper.unwrapCause(e))
}

/**
 * Extension function variant of [ActionListener.completeWith]
 */
inline fun <T> ActionListener<T>.completeWith(block : () -> T) {
    try {
        onResponse(block())
    } catch (e: Exception) {
        onFailure(e)
    }
}

/**
 * Stores and restores the Elasticsearch [ThreadContext] when the coroutine is suspended and resumed.
 *
 * The implementation is a little confusing because Elasticsearch and Kotlin uses [ThreadContext.stashContext] to
 * restore the default context.
 *
 * @param threadContext - a [ThreadContext] instance
 */
class ElasticThreadContextElement(private val threadContext: ThreadContext) : ThreadContextElement<Unit> {

    companion object Key : CoroutineContext.Key<ElasticThreadContextElement>
    private var context: ThreadContext.StoredContext = threadContext.newStoredContext(true)

    override val key: CoroutineContext.Key<*>
        get() = Key

    override fun restoreThreadContext(context: CoroutineContext, oldState: Unit) {
        this.context = threadContext.stashContext()
    }

    override fun updateThreadContext(context: CoroutineContext) = this.context.close()
}

class ElasticsearchClientThreadContextElement(private val threadContext: ThreadContext,
                                              private val replicationMetadata: ReplicationMetadata?,
                                              private val action: String,
                                              private val injectSecurityContext: Boolean,
                                              private val defaultContext: Boolean) : ThreadContextElement<Unit> {

    companion object Key : CoroutineContext.Key<ElasticThreadContextElement>

    private var context: ThreadContext.StoredContext = threadContext.newStoredContext(true)
    private var init = false

    override val key: CoroutineContext.Key<*>
        get() = Key

    override fun restoreThreadContext(cc: CoroutineContext, oldState: Unit) {
        // ES expects default context to be set after coroutine is suspended.
        this.context = threadContext.stashContext()
        init = true
    }

    override fun updateThreadContext(cc: CoroutineContext) {
        this.context.close()
        if(!init) {
            // To ensure, we initialize security related transients only once
            this.context = if(injectSecurityContext || defaultContext)
                threadContext.stashContext()
            else
                threadContext.newStoredContext(true)
            if(injectSecurityContext) {
                // Populate relevant transients from replication metadata
                SecurityContext.setBasedOnActions(replicationMetadata, action, threadContext)
            }
        }
    }
}

fun ThreadPool.coroutineContext() : CoroutineContext = ElasticThreadContextElement(threadContext)

fun ThreadPool.coroutineContext(replicationMetadata: ReplicationMetadata?, action: String,
                                injectSecurityContext: Boolean, defaultContext: Boolean) : CoroutineContext =
        ElasticsearchClientThreadContextElement(threadContext, replicationMetadata, action, injectSecurityContext, defaultContext)

/**
 * Captures the current Elastic [ThreadContext] in the coroutine context as well as sets the given executor as the dispatcher
 */
fun ThreadPool.coroutineContext(executorName: String) : CoroutineContext =
    executor(executorName).asCoroutineDispatcher() + coroutineContext()

suspend fun <T : MasterNodeRequest<T>> submitClusterStateUpdateTask(request: AcknowledgedRequest<T>,
                                                                    taskExecutor: ClusterStateTaskExecutor<AcknowledgedRequest<T>>,
                                                                    clusterService: ClusterService,
                                                                    source: String): ClusterState {
    return suspendCoroutine { continuation ->
        clusterService.submitStateUpdateTask(
                source,
                request,
                ClusterStateTaskConfig.build(Priority.NORMAL),
                taskExecutor,
                object : ClusterStateTaskListener {
                    override fun onFailure(source: String, e: java.lang.Exception) {
                        continuation.resumeWithException(e)
                    }

                    override fun clusterStateProcessed(source: String?, oldState: ClusterState?, newState: ClusterState) {
                        continuation.resume(newState)
                    }
                })
    }

}