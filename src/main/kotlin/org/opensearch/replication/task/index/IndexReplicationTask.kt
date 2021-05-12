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

package org.opensearch.replication.task.index

import org.opensearch.replication.ReplicationException
import org.opensearch.replication.action.index.block.IndexBlockUpdateType
import org.opensearch.replication.action.index.block.UpdateIndexBlockRequest
import org.opensearch.replication.action.stop.StopIndexReplicationAction
import org.opensearch.replication.action.stop.StopIndexReplicationRequest
import org.opensearch.replication.metadata.getReplicationStateParamsForIndex
import org.opensearch.replication.repository.REMOTE_SNAPSHOT_NAME
import org.opensearch.replication.repository.RemoteClusterRepository
import org.opensearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import org.opensearch.replication.task.CrossClusterReplicationTask
import org.opensearch.replication.task.ReplicationState
import org.opensearch.replication.task.shard.ShardReplicationExecutor
import org.opensearch.replication.task.shard.ShardReplicationParams
import org.opensearch.replication.task.shard.ShardReplicationTask
import org.opensearch.replication.util.suspending
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import org.opensearch.OpenSearchTimeoutException
import org.opensearch.ResourceNotFoundException
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.client.Client
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.ClusterStateObserver
import org.opensearch.cluster.RestoreInProgress
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.logging.Loggers
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.index.Index
import org.opensearch.index.shard.ShardId
import org.opensearch.persistent.PersistentTaskState
import org.opensearch.persistent.PersistentTasksCustomMetadata
import org.opensearch.persistent.PersistentTasksCustomMetadata.PersistentTask
import org.opensearch.persistent.PersistentTasksNodeService
import org.opensearch.persistent.PersistentTasksService
import org.opensearch.tasks.TaskId
import org.opensearch.threadpool.ThreadPool
import java.util.function.Predicate
import java.util.stream.Collectors
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import org.opensearch.replication.action.index.block.UpdateIndexBlockAction
import org.opensearch.replication.util.startTask
import org.opensearch.replication.util.suspendExecute
import org.opensearch.replication.util.waitForNextChange

class IndexReplicationTask(id: Long, type: String, action: String, description: String,
                           parentTask: TaskId,
                           executor: String,
                           clusterService: ClusterService,
                           threadPool: ThreadPool,
                           client: Client,
                           params: IndexReplicationParams,
                           private val persistentTasksService: PersistentTasksService)
    : CrossClusterReplicationTask(id, type, action, description, parentTask, emptyMap(), executor,
                                  clusterService, threadPool, client), ClusterStateListener {
    private lateinit var currentTaskState : IndexReplicationState
    private lateinit var followingTaskState : IndexReplicationState

    override val remoteCluster = params.remoteCluster

    private val remoteClient = client.getRemoteClusterClient(remoteCluster)
    val remoteIndex   = params.remoteIndex
    override val followerIndexName = params.followerIndexName

    override val log = Loggers.getLogger(javaClass, Index(params.followerIndexName, ClusterState.UNKNOWN_UUID))
    private val cso = ClusterStateObserver(clusterService, log, threadPool.threadContext)
    private val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(clusterService.clusterName.value(), remoteClient)

    private val SLEEP_TIME_BETWEEN_POLL_MS = 5000L

    override fun indicesOrShards(): List<Any> = listOf(followerIndexName)

    override suspend fun execute(initialState: PersistentTaskState?) {
        checkNotNull(initialState) { "Missing initial state" }
        followingTaskState = FollowingState(emptyMap())
        currentTaskState = initialState as IndexReplicationState
        while (scope.isActive) {
            val newState = when (currentTaskState.state) {
                ReplicationState.INIT -> {
                    addListenerToInterruptTask()
                    startRestore()
                }
                ReplicationState.RESTORING -> {
                    waitForRestore()
                }
                ReplicationState.INIT_FOLLOW -> {
                    startShardFollowTasks(emptyMap())
                }
                ReplicationState.FOLLOWING -> {
                    if (currentTaskState is FollowingState) {
                        followingTaskState = (currentTaskState as FollowingState)
                        addIndexBlockForReplication()
                    } else {
                        throw ReplicationException("Wrong state type: ${currentTaskState::class}")
                    }
                }
                ReplicationState.MONITORING -> {
                    pollShardTaskStatus((followingTaskState as FollowingState).shardReplicationTasks)
                }
                ReplicationState.FAILED -> {
                    stopReplicationTasks()
                    currentTaskState
                }
                ReplicationState.COMPLETED -> {
                    markAsCompleted()
                    CompletedState
                }
            }
            if (newState != currentTaskState) {
                currentTaskState = updateState(newState)
            }
            if (isCompleted) break
        }
    }

    private fun addListenerToInterruptTask() {
        clusterService.addListener(this)
    }

    private suspend fun pollShardTaskStatus(shardTasks: Map<ShardId, PersistentTask<ShardReplicationParams>>): IndexReplicationState {
        val failedShardTasks = findFailedShardTasks(shardTasks, clusterService.state())
        if (failedShardTasks.isNotEmpty())
            return FailedState(failedShardTasks, "At least one of the shard replication task has failed")
        delay(SLEEP_TIME_BETWEEN_POLL_MS)
        return MonitoringState
    }

    private suspend fun stopReplicationTasks() {
        val stopReplicationResponse = client.suspendExecute(StopIndexReplicationAction.INSTANCE, StopIndexReplicationRequest(followerIndexName))
        if (!stopReplicationResponse.isAcknowledged)
            throw ReplicationException("Failed to gracefully stop replication after one or more shard tasks failed. " +
                    "Replication tasks may need to be stopped manually.")
    }

    private fun findFailedShardTasks(shardTasks: Map<ShardId, PersistentTask<ShardReplicationParams>>, clusterState: ClusterState)
        :Map<ShardId, PersistentTask<ShardReplicationParams>> {

        val persistentTasks = clusterState.metadata.custom<PersistentTasksCustomMetadata>(PersistentTasksCustomMetadata.TYPE)
        val runningShardTasks = persistentTasks.findTasks(ShardReplicationExecutor.TASK_NAME, Predicate { true }).stream()
                .map { task -> task.params as ShardReplicationParams }
                .collect(Collectors.toList())
        return shardTasks.filterKeys { shardId ->
            runningShardTasks.find { task -> task.followerShardId == shardId } == null}
    }

    override suspend fun cleanup() {
        if (currentTaskState.state == ReplicationState.INIT || currentTaskState.state == ReplicationState.RESTORING)  {
            log.info("Replication stopped before restore could finish, so removing partial restore..")
            cancelRestore()
        }
        /* This is to minimise overhead of calling an additional listener as
         * it continues to be called even after the task is completed.
         */
        clusterService.removeListener(this)
    }

    private suspend fun addIndexBlockForReplication(): IndexReplicationState {
        val request = UpdateIndexBlockRequest(followerIndexName, IndexBlockUpdateType.ADD_BLOCK)
        client.suspendExecute(UpdateIndexBlockAction.INSTANCE, request)
        return MonitoringState
    }

    private suspend fun updateState(newState: IndexReplicationState) : IndexReplicationState {
        return suspendCoroutine { cont ->
            updatePersistentTaskState(newState, object : ActionListener<PersistentTask<*>> {
                override fun onFailure(e: Exception) {
                    cont.resumeWithException(e)
                }

                override fun onResponse(response: PersistentTask<*>) {
                    cont.resume(response.state as IndexReplicationState)
                }
            })
        }
    }

    private suspend fun
            startShardFollowTasks(tasks: Map<ShardId, PersistentTask<ShardReplicationParams>>): FollowingState {
        assert(clusterService.state().routingTable.hasIndex(followerIndexName)) { "Can't find index $followerIndexName" }
        val shards = clusterService.state().routingTable.indicesRouting().get(followerIndexName).shards()
        val newTasks = shards.map {
            it.value.shardId
        }.associate { shardId ->
            val task = tasks.getOrElse(shardId) {
                startReplicationTask(ShardReplicationParams(remoteCluster, ShardId(remoteIndex, shardId.id), shardId))
            }
            return@associate shardId to task
        }
        return FollowingState(newTasks)
    }

    private suspend fun cancelRestore() {
        /*
         * Should be safe to delete the retention leases here for all the shards
         * as the restore is not yet completed
         */
        val shards = clusterService.state().routingTable.indicesRouting().get(followerIndexName).shards()
        shards.forEach {
            val followerShardId = it.value.shardId
            retentionLeaseHelper.removeRetentionLease(ShardId(remoteIndex, followerShardId.id), followerShardId)
        }

        /* As given here
         * (https://www.elastic.co/guide/en/elasticsearch/reference/6.8/modules-snapshots.html#_stopping_currently_running_snapshot_and_restore_operations)
         * a snapshot restore can be cancelled by deleting the indices being restored.
         */
        log.info("Deleting the index $followerIndexName")
        suspending(client.admin().indices()::delete)(DeleteIndexRequest(followerIndexName))
    }

    private suspend fun startRestore(): IndexReplicationState {
        val restoreRequest = client.admin().cluster()
            .prepareRestoreSnapshot(RemoteClusterRepository.repoForCluster(remoteCluster), REMOTE_SNAPSHOT_NAME)
            .setIndices(remoteIndex.name)
            .request()
        if (remoteIndex.name != followerIndexName) {
            restoreRequest.renamePattern(remoteIndex.name)
                .renameReplacement(followerIndexName)
        }
        val response = suspending(client.admin().cluster()::restoreSnapshot)(restoreRequest)
        if (response.restoreInfo != null) {
            if (response.restoreInfo.failedShards() != 0) {
                throw ReplicationException("Restore failed: $response")
            }
            return FollowingState(emptyMap())
        }
        cso.waitForNextChange("remote restore start") { inProgressRestore(it) != null }
        return RestoreState
    }

    private suspend fun waitForRestore(): IndexReplicationState {
        var restore = inProgressRestore() ?: throw ResourceNotFoundException("""
            Unable to find in progress restore for remote index: $remoteCluster:$remoteIndex. 
            This can happen if there was a badly timed master node failure. 
        """.trimIndent())
        while (restore.state() != RestoreInProgress.State.FAILURE && restore.state() != RestoreInProgress.State.SUCCESS) {
            try {
                cso.waitForNextChange("remote restore finish")
            } catch(e: OpenSearchTimeoutException) {
                log.info("Waiting for restore to complete")
            }
            restore = inProgressRestore() ?: throw ResourceNotFoundException("""
            Unable to find in progress restore for remote index: $remoteCluster:$remoteIndex. 
            This can happen if there was a badly timed master node failure. 
        """.trimIndent())
        }

        if (restore.state() == RestoreInProgress.State.FAILURE) {
            val failureReason = restore.shards().values().find {
                it.value.state() == RestoreInProgress.State.FAILURE
            }!!.value.reason()
            throw ReplicationException("Remote restore failed: $failureReason")
        } else {
            return InitFollowState
        }
    }

    private fun inProgressRestore(cs: ClusterState = clusterService.state()): RestoreInProgress.Entry? {
        return cs.custom<RestoreInProgress>(RestoreInProgress.TYPE).singleOrNull { entry ->
            entry.snapshot().repository == RemoteClusterRepository.repoForCluster(remoteCluster) &&
                entry.indices().singleOrNull { idx -> idx == followerIndexName } != null
        }
    }

    private suspend
    fun startReplicationTask(replicationParams : ShardReplicationParams) : PersistentTask<ShardReplicationParams> {
        return persistentTasksService.startTask(ShardReplicationTask.taskIdForShard(replicationParams.followerShardId),
            ShardReplicationExecutor.TASK_NAME, replicationParams)
    }

    override fun clusterChanged(event: ClusterChangedEvent) {
        log.debug("Cluster metadata listener invoked on index task...")
        if (event.metadataChanged()) {
            val replicationStateParams = getReplicationStateParamsForIndex(clusterService, followerIndexName)
            if (replicationStateParams == null) {
                if (PersistentTasksNodeService.Status(State.STARTED) == status)
                    scope.cancel("Index replication task received an interrupt.")
            }
        }
    }

    override fun replicationTaskResponse(): CrossClusterReplicationTaskResponse {
        return IndexReplicationTaskResponse(currentTaskState)
    }

    class IndexReplicationTaskResponse(private val taskState : IndexReplicationState):
            CrossClusterReplicationTaskResponse(ReplicationState.COMPLETED.name), ToXContentObject {

        override fun writeTo(out: StreamOutput) {
            super.writeTo(out)
            taskState.writeTo(out)
        }

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            var responseBuilder = builder.startObject()
                    .field("index_task_status", ReplicationState.COMPLETED.name)
                    .field("following_tasks")
            return taskState.toXContent(responseBuilder, params).endObject()
        }
    }
}

