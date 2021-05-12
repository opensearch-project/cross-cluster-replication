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

package org.opensearch.replication.task.shard

import org.opensearch.replication.metadata.REPLICATION_OVERALL_STATE_KEY
import org.opensearch.replication.metadata.REPLICATION_OVERALL_STATE_RUNNING_VALUE
import org.opensearch.replication.metadata.getReplicationStateParamsForIndex
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchException
import org.opensearch.client.Client
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.service.ClusterService
import org.opensearch.persistent.AllocatedPersistentTask
import org.opensearch.persistent.PersistentTaskState
import org.opensearch.persistent.PersistentTasksCustomMetadata.Assignment
import org.opensearch.persistent.PersistentTasksCustomMetadata.PersistentTask
import org.opensearch.persistent.PersistentTasksExecutor
import org.opensearch.tasks.TaskId
import org.opensearch.threadpool.ThreadPool

class ShardReplicationExecutor(executor: String, private val clusterService : ClusterService,
                               private val threadPool: ThreadPool, private val client: Client) :
    PersistentTasksExecutor<ShardReplicationParams>(TASK_NAME, executor) {

    companion object {
        const val TASK_NAME = "cluster:indices/shards/replication"
        val SHARD_NOT_ACTIVE = Assignment(null, "No active shard found")
        val log = LogManager.getLogger(ShardReplicationExecutor::class.java)
    }

    override fun validate(params: ShardReplicationParams, clusterState: ClusterState) {
        // Checks that there is a primary shard. Side-effect will check that the index and shard exists.
        clusterState.routingTable.shardRoutingTable(params.followerShardId)
            .primaryShard() ?: throw OpenSearchException("no primary shard available for ${params.followerShardId}")
        val replicationStateParams = getReplicationStateParamsForIndex(clusterService, params.followerShardId.indexName)
                ?:
            throw IllegalStateException("Cant find replication details metadata for followIndex:${params.followerShardId.indexName}. " +
                    "Seems like replication is not in progress, so not starting shard task for shardId:${params.followerShardId}")
        if (replicationStateParams[REPLICATION_OVERALL_STATE_KEY] != REPLICATION_OVERALL_STATE_RUNNING_VALUE)
            throw IllegalStateException("Unknown replication state metadata:${replicationStateParams[REPLICATION_OVERALL_STATE_KEY]} " +
                    " followIndex:${params.followerShardId.indexName}")
    }

    override fun getAssignment(params: ShardReplicationParams, clusterState: ClusterState) : Assignment {
        val primaryShard = clusterState.routingTable().shardRoutingTable(params.followerShardId).primaryShard()
        if (!primaryShard.active()) return SHARD_NOT_ACTIVE
        return Assignment(primaryShard.currentNodeId(), "node with primary shard")
    }

    override fun nodeOperation(task: AllocatedPersistentTask, params: ShardReplicationParams, state: PersistentTaskState?) {
        if (task is ShardReplicationTask) {
            log.info("starting persistent replication task: $params, $state, ${task.allocationId}, ${task.status}")
            task.run()
        } else {
            task.markAsFailed(IllegalArgumentException("Unknown task class ${task::class.java}"))
        }
    }

    override fun createTask(id: Long, type: String, action: String, parentTaskId: TaskId,
                            taskInProgress: PersistentTask<ShardReplicationParams>,
                            headers: Map<String, String>): AllocatedPersistentTask {
        return ShardReplicationTask(id, type, action, getDescription(taskInProgress), parentTaskId,
                                    taskInProgress.params!!, executor, clusterService, threadPool, client)
    }

    override fun getDescription(taskInProgress: PersistentTask<ShardReplicationParams>): String {
        val params = requireNotNull(taskInProgress.params)
        return "replication:${params.remoteCluster}:${params.remoteShardId} -> ${params.followerShardId}"
    }
}
