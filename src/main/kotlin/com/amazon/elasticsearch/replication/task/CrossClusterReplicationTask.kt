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

package com.amazon.elasticsearch.replication.task

import com.amazon.elasticsearch.replication.util.SecurityContext
import com.amazon.elasticsearch.replication.util.coroutineContext
import com.amazon.elasticsearch.replication.util.suspending
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.logging.log4j.Logger
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.persistent.AllocatedPersistentTask
import org.elasticsearch.persistent.PersistentTaskState
import org.elasticsearch.persistent.PersistentTasksService
import org.elasticsearch.tasks.TaskId
import org.elasticsearch.tasks.TaskManager
import org.elasticsearch.threadpool.ThreadPool

abstract class CrossClusterReplicationTask(id: Long, type: String, action: String, description: String, parentTask: TaskId,
                                           headers: Map<String, String>,
                                           protected val executor: String,
                                           protected val clusterService: ClusterService,
                                           protected val threadPool: ThreadPool,
                                           protected val client: Client) :
    AllocatedPersistentTask(id, type, action, description, parentTask, headers) {

    protected val scope = CoroutineScope(threadPool.coroutineContext(executor))
    protected abstract val log : Logger
    protected abstract val followerIndexName: String
    protected abstract val remoteCluster: String
    @Volatile private lateinit var taskManager: TaskManager

    override fun init(persistentTasksService: PersistentTasksService, taskManager: TaskManager,
                      persistentTaskId: String, allocationId: Long) {
        super.init(persistentTasksService, taskManager, persistentTaskId, allocationId)
        this.taskManager = taskManager
    }

    override fun onCancelled() {
        super.onCancelled()
        scope.cancel()
    }

    fun run(initialState: PersistentTaskState? = null) {
        scope.launch {
            var exception : Throwable? = null
            try {
                registerCloseListeners()
                setSecurityContext()
                execute(initialState)
                markAsCompleted()
            } catch (e: Exception) {
                if (isCancelled || e is CancellationException) {
                    markAsCompleted()
                    log.info("Completed the task with id:$id")
                } else {
                    exception = e
                    markAsFailed(e)
                }
            } finally {
                unregisterCloseListeners()
                // Need to execute cleanup regardless of cancellation so run in NonCancellable context but with a
                // timeout. See https://kotlinlang.org/docs/reference/coroutines/cancellation-and-timeouts.html#run-non-cancellable-block
                withContext(NonCancellable) {
                    withTimeoutOrNull(60000) {
                        cleanupFinally(exception)
                    }
                }
            }
        }
    }

    protected abstract fun replicationTaskResponse(): CrossClusterReplicationTaskResponse

    override fun markAsCompleted() {
        taskManager.storeResult(this, replicationTaskResponse(), ActionListener.wrap(
                {log.info("Successfully persisted task status")},
                {e -> log.warn("Error storing result $e")}
        ))
        super.markAsCompleted()
    }

    override fun markAsFailed(e: Exception) {
        taskManager.storeResult(this, e, ActionListener.wrap(
                {log.info("Successfully persisted failure")},
                {log.error("Task failed due to $e")}
        ))
        super.markAsFailed(e)
    }

    /**
     * A list of [ShardId]s or index names for which this task's [onIndexOrShardClosed] method should be called when
     * closed.
     */
    protected open fun indicesOrShards() : List<Any> = emptyList()

    private fun registerCloseListeners() {
        for (indexOrShard in indicesOrShards()) {
            IndexCloseListener.addCloseListener(indexOrShard, this)
        }
    }

    private fun unregisterCloseListeners() {
        for (indexOrShard in indicesOrShards()) {
            IndexCloseListener.removeCloseListener(indexOrShard, this)
        }
    }

    fun onIndexOrShardClosed(indexOrShardId: Any) {
        scope.cancel("$indexOrShardId was closed.")
    }

    /**
     * Persists the state of the task in the cluster metadata.  If the task is resumed on a different node then this
     * will be used to restart the task from the correct state.
     */
    protected suspend fun updateTaskState(state: PersistentTaskState) {
        suspending(::updatePersistentTaskState)(state)
    }

    protected abstract suspend fun execute(initialState: PersistentTaskState?)

    protected open suspend fun cleanup() {}

    /**
     * Handles case where a suspending finally block throws an exception.
     */
    private suspend fun cleanupFinally(cause: Throwable?) {
        if (cause == null) {
            cleanup()
        } else {
            try {
                cleanup()
            } catch(e: Exception) {
                cause.addSuppressed(e)
            }
        }
    }

    /**
     * Sets the security context
     */
    protected open fun setSecurityContext() {
        val injectedUser = SecurityContext.fromClusterState(clusterService.state(), remoteCluster, followerIndexName)
        SecurityContext.toThreadContext(threadPool.threadContext, injectedUser)
    }

    open class CrossClusterReplicationTaskResponse(val status: String): ActionResponse(), ToXContentObject {
        override fun writeTo(out: StreamOutput) {
            out.writeString(status)
        }

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            return builder.startObject()
                    .field("status", status)
                    .endObject()
        }
    }
}
