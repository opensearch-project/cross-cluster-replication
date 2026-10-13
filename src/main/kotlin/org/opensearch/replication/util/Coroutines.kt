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

package org.opensearch.replication.util

import org.opensearch.replication.metadata.store.ReplicationMetadata
import kotlinx.coroutines.*
import org.opensearch.OpenSearchTimeoutException
import org.opensearch.ExceptionsHelper
import org.opensearch.core.action.ActionListener
import org.opensearch.action.ActionRequest
import org.opensearch.core.action.ActionResponse
import org.opensearch.action.ActionType
import org.opensearch.action.support.clustermanager.AcknowledgedRequest
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.OpenSearchClient
import org.opensearch.cluster.*
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.Priority
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.index.shard.GlobalCheckpointListeners
import org.opensearch.index.shard.IndexShard
import org.opensearch.persistent.PersistentTaskParams
import org.opensearch.persistent.PersistentTasksCustomMetadata.PersistentTask
import org.opensearch.persistent.PersistentTasksService
import org.opensearch.threadpool.ThreadPool
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
        OpenSearchClient.suspendExecute(action: ActionType<Resp>, req: Req,
                                           injectSecurityContext: Boolean = false,
                                           defaultContext: Boolean = false) : Resp {
    return withContext(this.threadPool().coroutineContext(null, action.name(), injectSecurityContext, defaultContext)) {
        suspendCancellableCoroutine<Resp> { cont -> execute(action, req, CoroutineActionListener(cont)) }
    }
}

suspend fun <Req: ActionRequest, Resp: ActionResponse>
        OpenSearchClient.suspendExecute(replicationMetadata: ReplicationMetadata,
                                           action: ActionType<Resp>, req: Req, defaultContext: Boolean = false) : Resp {
    return withContext(this.threadPool().coroutineContext(replicationMetadata, action.name(), true, defaultContext)) {
        suspendCancellableCoroutine<Resp> { cont ->
            execute(action, req, CoroutineActionListener(cont))
        }
    }
}

suspend fun <Req: ActionRequest, Resp: ActionResponse>
        OpenSearchClient.suspendExecute(replicationMetadata: ReplicationMetadata?,
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
                    e is TimeoutException -> cont.resumeWithException(OpenSearchTimeoutException(e.message))
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
                cont.resumeWithException(OpenSearchTimeoutException("timed out waiting for $reason"))
            }
        }, predicate,  TimeValue(60000))
    }
}

suspend fun <T> ClusterService.waitForClusterStateUpdate(source: String,
                                                         updateTaskFactory: (ActionListener<T>) ->
                                                         AckedClusterStateUpdateTask<T>) : T =
    suspendCoroutine { cont -> submitStateUpdateTask(source, updateTaskFactory(CoroutineActionListener(cont))) }

/**
 * Batching-aware variant of [waitForClusterStateUpdate].
 *
 * Uses the 5-arg [ClusterService.submitStateUpdateTask] overload so that concurrent submissions
 * with the same [executor] instance coalesce into a single cluster-manager update turn via
 * OpenSearch's [org.opensearch.cluster.service.TaskBatcher]. The caller is responsible for
 * ensuring [executor] is a shared singleton — passing a fresh executor per invocation is
 * functionally equivalent to the non-batching overload above (unique batching key -> no coalescing).
 *
 * The per-submission ack semantics of [AckedClusterStateUpdateTask] are preserved by wrapping
 * the coroutine continuation in a [BatchedCoroutineAckListener], which implements both
 * [ClusterStateTaskListener] (for immediate failures and no-longer-cluster-manager) and
 * [AckedClusterStateTaskListener] (to resume with an [AcknowledgedResponse] only after the new
 * state is acknowledged by all nodes, mirroring the behavior of AckedClusterStateUpdateTask).
 *
 * @param source diagnostic label for the update, used in cluster manager logging
 * @param task the per-caller task input; identity-distinct across submissions
 * @param executor shared singleton executor implementing coalesced [ClusterStateTaskExecutor.execute]
 * @param priority priority of the task in the cluster manager's priority queue
 * @param taskTimeout queue timeout — if the task waits in the queue longer than this, it fails
 *                    with [org.opensearch.cluster.service.ClusterManagerService.ProcessClusterEventTimeoutException].
 *                    Null (default) means no queue timeout.
 * @param ackTimeout time to wait for all-nodes-acked before firing onAckTimeout. Defaults to 30s.
 */
suspend fun <T> ClusterService.waitForClusterStateUpdate(
        source: String,
        task: T,
        executor: ClusterStateTaskExecutor<T>,
        priority: Priority = Priority.NORMAL,
        taskTimeout: TimeValue? = null,
        ackTimeout: TimeValue = DEFAULT_ACK_TIMEOUT
) : AcknowledgedResponse = suspendCoroutine { cont ->
    val listener = BatchedCoroutineAckListener(cont, ackTimeout)
    val config = ClusterStateTaskConfig.build(priority, taskTimeout)
    submitStateUpdateTask(source, task, config, executor, listener)
}

private val DEFAULT_ACK_TIMEOUT: TimeValue = TimeValue.timeValueSeconds(30)

/**
 * Per-submission listener for the batched-executor code path. Implements both
 * [ClusterStateTaskListener] and [AckedClusterStateTaskListener] so that the framework will:
 *
 *   1. Call [onFailure] if the executor's execute() threw for this task, or if the CM stepped down.
 *   2. Call [clusterStateProcessed] after the CM applies the new state locally (no-op here — we
 *      wait for all-nodes-acked before resuming, matching AckedClusterStateUpdateTask semantics).
 *   3. Call [onAllNodesAcked] (with null exception on success, or with the exception on ack failure).
 *   4. Call [onAckTimeout] if some nodes have not acked within [ackTimeout].
 *
 * The listener is one-shot: whichever callback fires first resumes the coroutine, and subsequent
 * callbacks are dropped. This guards against a theoretical race where both [onFailure] and an ack
 * callback could otherwise resume the same continuation.
 */
class BatchedCoroutineAckListener(
        private val continuation: Continuation<AcknowledgedResponse>,
        private val ackTimeoutValue: TimeValue
) : AckedClusterStateTaskListener {

    @Volatile private var resumed: Boolean = false

    @Synchronized
    private fun resumeOnce(response: AcknowledgedResponse) {
        if (resumed) return
        resumed = true
        continuation.resume(response)
    }

    @Synchronized
    private fun resumeOnceWithException(e: Exception) {
        if (resumed) return
        resumed = true
        continuation.resumeWithException(ExceptionsHelper.unwrapCause(e))
    }

    override fun onFailure(source: String, e: Exception) {
        resumeOnceWithException(e)
    }

    override fun clusterStateProcessed(source: String, oldState: ClusterState, newState: ClusterState) {
        // Local state is applied. Wait for onAllNodesAcked before resuming, matching
        // AckedClusterStateUpdateTask.clusterStateProcessed (a no-op there too).
    }

    override fun mustAck(discoveryNode: DiscoveryNode): Boolean = true

    override fun onAllNodesAcked(e: Exception?) {
        // Mirror AckedClusterStateUpdateTask.onAllNodesAcked: acknowledged = (e == null).
        resumeOnce(AcknowledgedResponse(e == null))
    }

    override fun onAckTimeout() {
        // Mirror AckedClusterStateUpdateTask.onAckTimeout: acknowledged = false on timeout.
        resumeOnce(AcknowledgedResponse(false))
    }

    override fun ackTimeout(): TimeValue = ackTimeoutValue
}

suspend fun <T : PersistentTaskParams>
    PersistentTasksService.startTask(taskId: String, taskName: String, params : T): PersistentTask<T> {
    return suspendCoroutine { cont -> this.sendStartRequest(taskId, taskName, params, CoroutineActionListener(cont)) }
}

suspend fun PersistentTasksService.removeTask(taskId: String): PersistentTask<*> {
    return suspendCoroutine { cont -> this.sendRemoveRequest(taskId, CoroutineActionListener(cont)) }
}

suspend fun PersistentTasksService.waitForTaskCondition(taskId: String, timeout: TimeValue,
                                                        condition: (PersistentTask<*>?) -> Boolean) : PersistentTask<*> {
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
 * Stores and restores the OpenSearch [ThreadContext] when the coroutine is suspended and resumed.
 *
 * The implementation is a little confusing because OpenSearch and Kotlin uses [ThreadContext.stashContext] to
 * restore the default context.
 *
 * @param threadContext - a [ThreadContext] instance
 */
class OpenSearchThreadContextElement(private val threadContext: ThreadContext) : ThreadContextElement<Unit> {

    companion object Key : CoroutineContext.Key<OpenSearchThreadContextElement>
    private var context: ThreadContext.StoredContext = threadContext.newStoredContext(true)

    override val key: CoroutineContext.Key<*>
        get() = Key

    override fun restoreThreadContext(context: CoroutineContext, oldState: Unit) {
        this.context = threadContext.stashContext()
    }

    override fun updateThreadContext(context: CoroutineContext) = this.context.close()
}

class OpenSearchClientThreadContextElement(private val threadContext: ThreadContext,
                                              private val replicationMetadata: ReplicationMetadata?,
                                              private val action: String,
                                              private val injectSecurityContext: Boolean,
                                              private val defaultContext: Boolean) : ThreadContextElement<Unit> {

    companion object Key : CoroutineContext.Key<OpenSearchThreadContextElement>

    private var storedContext: ThreadContext.StoredContext = threadContext.newStoredContext(true)
    private var init = false

    override val key: CoroutineContext.Key<*>
        get() = Key

    override fun restoreThreadContext(context: CoroutineContext, oldState: Unit) {
        // OpenSearch expects default context to be set after coroutine is suspended.
        this.storedContext = threadContext.stashContext()
        init = true
    }

    override fun updateThreadContext(context: CoroutineContext) {
        this.storedContext.close()
        if(!init) {
            // To ensure, we initialize security related transients only once
            this.storedContext = if(injectSecurityContext || defaultContext)
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

fun ThreadPool.coroutineContext() : CoroutineContext = OpenSearchThreadContextElement(threadContext)

fun ThreadPool.coroutineContext(replicationMetadata: ReplicationMetadata?, action: String,
                                injectSecurityContext: Boolean, defaultContext: Boolean) : CoroutineContext =
        OpenSearchClientThreadContextElement(threadContext, replicationMetadata, action, injectSecurityContext, defaultContext)

/**
 * Captures the current OpenSearch [ThreadContext] in the coroutine context as well as sets the given executor as the dispatcher
 */
fun ThreadPool.coroutineContext(executorName: String) : CoroutineContext =
    executor(executorName).asCoroutineDispatcher() + coroutineContext()

suspend fun <T : ClusterManagerNodeRequest<T>> submitClusterStateUpdateTask(request: AcknowledgedRequest<T>,
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