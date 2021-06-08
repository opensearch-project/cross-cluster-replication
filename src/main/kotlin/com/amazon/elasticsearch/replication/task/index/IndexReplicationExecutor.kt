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

package com.amazon.elasticsearch.replication.task.index

import com.amazon.elasticsearch.replication.metadata.REPLICATION_OVERALL_STATE_KEY
import com.amazon.elasticsearch.replication.metadata.REPLICATION_OVERALL_STATE_RUNNING_VALUE
import com.amazon.elasticsearch.replication.metadata.getReplicationStateParamsForIndex
import com.amazon.elasticsearch.replication.util.persistentTasksService
import org.apache.logging.log4j.LogManager
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.persistent.AllocatedPersistentTask
import org.elasticsearch.persistent.PersistentTaskState
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask
import org.elasticsearch.persistent.PersistentTasksExecutor
import org.elasticsearch.tasks.TaskId
import org.elasticsearch.threadpool.ThreadPool

class IndexReplicationExecutor(executor: String, private val clusterService: ClusterService,
                               private val threadPool: ThreadPool, private val client: Client)
    : PersistentTasksExecutor<IndexReplicationParams>(TASK_NAME, executor) {

    companion object {
        const val TASK_NAME = "cluster:indices/admin/replication"
        val INITIAL_STATE = InitialState
        val log = LogManager.getLogger(IndexReplicationExecutor::class.java)
    }

    override fun validate(params: IndexReplicationParams, clusterState: ClusterState) {
        if (clusterState.routingTable.hasIndex(params.followerIndexName)) {
            throw IllegalArgumentException("Cant use same index again for replication. Either close or " +
                    "delete the index:${params.followerIndexName}")
        }
        val replicationStateParams = getReplicationStateParamsForIndex(clusterService, params.followerIndexName)
                ?:
                throw IllegalStateException("Index task started without replication state in cluster metadata")
        if (replicationStateParams[REPLICATION_OVERALL_STATE_KEY] != REPLICATION_OVERALL_STATE_RUNNING_VALUE) {
            throw IllegalArgumentException("Replication state for index:${params.followerIndexName} should be RUNNING, " +
                    "but was:${replicationStateParams[REPLICATION_OVERALL_STATE_KEY]}")
        }
    }

    override fun nodeOperation(task: AllocatedPersistentTask, params: IndexReplicationParams,
                               state: PersistentTaskState?) {
        if (task is IndexReplicationTask) {
            task.run(state ?: INITIAL_STATE)
        } else {
            task.markAsFailed(IllegalArgumentException("Unknown task class ${task::class.java}"))
        }
    }

    override fun createTask(id: Long, type: String, action: String, parentTaskId: TaskId,
                            taskInProgress: PersistentTask<IndexReplicationParams>,
                            headers: MutableMap<String, String>?): AllocatedPersistentTask {
        return IndexReplicationTask(id, type, action, getDescription(taskInProgress), parentTaskId,
                                    executor, clusterService, threadPool, client, requireNotNull(taskInProgress.params),
                                    persistentTasksService)
    }

    override fun getDescription(taskInProgress: PersistentTask<IndexReplicationParams>): String {
        val params = requireNotNull(taskInProgress.params)
        return "replication:${params.remoteCluster}:${params.remoteIndex} -> ${params.followerIndexName}"
    }
}