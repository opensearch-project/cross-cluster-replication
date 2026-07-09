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
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.ObsoleteCoroutinesApi
import org.apache.logging.log4j.Logger
import org.opensearch.OpenSearchException
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.transport.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.common.settings.Settings
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.index.IndexService
import org.opensearch.index.shard.IndexShard
import org.opensearch.core.index.shard.ShardId
import org.opensearch.indices.cluster.IndicesClusterStateService
import org.opensearch.persistent.AllocatedPersistentTask
import org.opensearch.persistent.PersistentTaskState
import org.opensearch.persistent.PersistentTasksService
import org.opensearch.replication.util.stackTraceToString
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.tasks.TaskId
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

    companion object {
        const val DEFAULT_WAIT_ON_ERRORS = 60000L
    }

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
                waitAndSetReplicationMetadata()
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
                {e -> log.warn("Error storing result ${e.stackTraceToString()}")}
        ))
        super.markAsCompleted()
    }

    override fun markAsFailed(e: Exception) {
        taskManager.storeResult(this, e, ActionListener.wrap(
                {log.info("Successfully persisted failure")},
                {log.error("Task failed due to ${e.stackTraceToString()}")}
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

    open fun onIndexShardClosed(shardId: ShardId, indexShard: IndexShard?, indexSettings: Settings) {
        // Replication task are tied to shards/index on the node.
        // On shard closed event, task running on the node can be cancelled
        // Sub classes can override this to take action/cancel the task
        cancelTask("$shardId was closed.")
    }

    open fun onIndexRemoved(indexService: IndexService,
                            reason: IndicesClusterStateService.AllocatedIndices.IndexRemovalReason) {
        // Replication task are tied to shards/index on the node.
        // On index removed event, task running on the node can be cancelled
        // Sub classes can override this to take action/cancel the task
        cancelTask("${indexService.index().name} was closed.")
    }

    /**
     * Persists the state of the task in the cluster metadata.  If the task is resumed on a different node then this
     * will be used to restart the task from the correct state.
     */
    protected suspend fun updateTaskState(state: PersistentTaskState) {
        client.suspending(::updatePersistentTaskState)(state)
    }

    @ObsoleteCoroutinesApi
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

    private suspend fun waitAndSetReplicationMetadata() {
        if (this::replicationMetadata.isInitialized) {
            return
        } else {
            while(overallTaskScope.isActive) {
                try {
                    setReplicationMetadata()
                    return
                } catch (e: OpenSearchException) {
                    if(e.status().status < 500 && e.status() != RestStatus.TOO_MANY_REQUESTS) {
                        throw e
                    }
                    log.error("Failed to fetch replication metadata due to ", e)
                    delay(DEFAULT_WAIT_ON_ERRORS)
                }
            }
        }
    }

    /**
     * Sets the security context
     */
    protected abstract suspend fun setReplicationMetadata()

    //used only in testing
    open suspend fun setReplicationMetadata(rm :ReplicationMetadata) {
        replicationMetadata = rm
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
