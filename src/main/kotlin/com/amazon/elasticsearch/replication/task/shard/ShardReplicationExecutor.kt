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

package com.amazon.elasticsearch.replication.task.shard

import com.amazon.elasticsearch.replication.ReplicationSettings
import com.amazon.elasticsearch.replication.metadata.ReplicationMetadataManager
import com.amazon.elasticsearch.replication.metadata.ReplicationOverallState
import com.amazon.elasticsearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import com.amazon.elasticsearch.replication.metadata.state.getReplicationStateParamsForIndex
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.persistent.AllocatedPersistentTask
import org.elasticsearch.persistent.PersistentTaskState
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask
import org.elasticsearch.persistent.PersistentTasksExecutor
import org.elasticsearch.tasks.TaskId
import org.elasticsearch.threadpool.ThreadPool

class ShardReplicationExecutor(executor: String, private val clusterService : ClusterService,
                               private val threadPool: ThreadPool, private val client: Client,
                               private val replicationMetadataManager: ReplicationMetadataManager,
                               private val replicationSettings: ReplicationSettings) :
    PersistentTasksExecutor<ShardReplicationParams>(TASK_NAME, executor) {

    companion object {
        const val TASK_NAME = "cluster:indices/shards/replication"
        val SHARD_NOT_ACTIVE = Assignment(null, "No active shard found")
        val log = LogManager.getLogger(ShardReplicationExecutor::class.java)
    }

    override fun validate(params: ShardReplicationParams, clusterState: ClusterState) {
        // Checks that there is a primary shard. Side-effect will check that the index and shard exists.
        clusterState.routingTable.shardRoutingTable(params.followerShardId)
            .primaryShard() ?: throw ElasticsearchException("no primary shard available for ${params.followerShardId}")
        val replicationStateParams = getReplicationStateParamsForIndex(clusterService, params.followerShardId.indexName)
                ?:
            throw IllegalStateException("Cant find replication details metadata for followIndex:${params.followerShardId.indexName}. " +
                    "Seems like replication is not in progress, so not starting shard task for shardId:${params.followerShardId}")
        if (replicationStateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE] != ReplicationOverallState.RUNNING.name)
            throw IllegalStateException("Unknown replication state metadata:${replicationStateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE]} " +
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
                                    taskInProgress.params!!, executor, clusterService, threadPool,
                                    client, replicationMetadataManager, replicationSettings)
    }

    override fun getDescription(taskInProgress: PersistentTask<ShardReplicationParams>): String {
        val params = requireNotNull(taskInProgress.params)
        return "replication:${params.remoteCluster}:${params.remoteShardId} -> ${params.followerShardId}"
    }
}
