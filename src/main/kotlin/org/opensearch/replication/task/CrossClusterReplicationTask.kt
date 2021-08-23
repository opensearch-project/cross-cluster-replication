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

package org.opensearch.replication.task

import org.opensearch.replication.ReplicationSettings
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.metadata.store.ReplicationMetadata
import org.opensearch.replication.task.autofollow.AutoFollowTask
import org.opensearch.replication.task.shard.ShardReplicationTask
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.suspending
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.cancel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.logging.log4j.Logger
import org.opensearch.action.ActionListener
import org.opensearch.action.ActionResponse
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.index.shard.ShardId
import org.opensearch.persistent.AllocatedPersistentTask
import org.opensearch.persistent.PersistentTaskState
import org.opensearch.persistent.PersistentTasksService
import org.opensearch.tasks.TaskId
import org.opensearch.tasks.TaskManager
import org.opensearch.threadpool.ThreadPool

abstract class CrossClusterReplicationTask(id: Long, type: String, action: String, description: String, parentTask: TaskId,
                                           headers: Map<String, String>,
                                           protected val executor: String,
                                           protected val clusterService: ClusterService,
                                           protected val threadPool: ThreadPool,
                                           protected val client: Client,
                                           protected val replicationMetadataManager: ReplicationMetadataManager,
                                           protected val replicationSettings: ReplicationSettings) :
    AllocatedPersistentTask(id, type, action, description, parentTask, headers) {

    private val overallTaskScope = CoroutineScope(threadPool.coroutineContext(executor))
    protected abstract val log : Logger
    protected abstract val followerIndexName: String
    protected abstract val leaderAlias: String
    protected lateinit var replicationMetadata: ReplicationMetadata
    @Volatile private lateinit var taskManager: TaskManager

    override fun init(persistentTasksService: PersistentTasksService, taskManager: TaskManager,
                      persistentTaskId: String, allocationId: Long) {
        super.init(persistentTasksService, taskManager, persistentTaskId, allocationId)
        this.taskManager = taskManager
    }

    override fun onCancelled() {
        super.onCancelled()
        cancelTask("Cancelled by OpenSearch Cancellable Task invocation.")
    }

    protected fun cancelTask(message: String) {
        overallTaskScope.cancel(message)
    }

    fun run(initialState: PersistentTaskState? = null) {
        overallTaskScope.launch {
            var exception : Throwable? = null
            try {
                registerCloseListeners()
                setReplicationMetadata()
                execute(this, initialState)
                markAsCompleted()
            } catch (e: Exception) {
                log.error(
                    "Exception encountered in CrossClusterReplicationTask - coroutine:isActive=${isActive} Context=${coroutineContext}", e)
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
        log.info("Going to mark ${this.javaClass.simpleName}:${this.id} task as completed")
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

    protected fun registerCloseListeners() {
        for (indexOrShard in indicesOrShards()) {
            IndexCloseListener.addCloseListener(indexOrShard, this)
        }
    }

    protected fun unregisterCloseListeners() {
        for (indexOrShard in indicesOrShards()) {
            IndexCloseListener.removeCloseListener(indexOrShard, this)
        }
    }

    fun onIndexShardClosed(indexOrShardId: Any) {
        if (this is ShardReplicationTask) {
            log.info("Cancelling shard replication task ")
            cancelTask("$indexOrShardId was closed.")
        }
    }

    fun onIndexClosed(indexOrShardId: Any) {
        cancelTask("$indexOrShardId was closed.")
    }

    /**
     * Persists the state of the task in the cluster metadata.  If the task is resumed on a different node then this
     * will be used to restart the task from the correct state.
     */
    protected suspend fun updateTaskState(state: PersistentTaskState) {
        client.suspending(::updatePersistentTaskState)(state)
    }

    protected abstract suspend fun execute(scope: CoroutineScope, initialState: PersistentTaskState?)

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
    protected open suspend fun setReplicationMetadata() {
        replicationMetadata = if(this is AutoFollowTask) {
            replicationMetadataManager.getAutofollowMetadata(followerIndexName, leaderAlias, fetch_from_primary = true)
        }
        else {
            replicationMetadataManager.getIndexReplicationMetadata(followerIndexName, fetch_from_primary = true)
        }
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
