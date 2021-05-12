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

package org.opensearch.replication.task.autofollow

import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.persistent.AllocatedPersistentTask
import org.opensearch.persistent.PersistentTaskState
import org.opensearch.persistent.PersistentTasksCustomMetadata.PersistentTask
import org.opensearch.persistent.PersistentTasksExecutor
import org.opensearch.tasks.TaskId
import org.opensearch.threadpool.ThreadPool

class AutoFollowExecutor(executor: String, private val clusterService: ClusterService,
                         private val threadPool: ThreadPool, private val client: Client) :
    PersistentTasksExecutor<AutoFollowParams>(TASK_NAME, executor) {

    companion object {
        const val TASK_NAME = "cluster:opendistro/admin/replication/autofollow"
    }

    override fun nodeOperation(task: AllocatedPersistentTask, params: AutoFollowParams, state: PersistentTaskState?) {
        if (task is AutoFollowTask) {
            task.run()
        } else {
            task.markAsFailed(IllegalArgumentException("unknown task type : ${task::class.java}"))
        }
    }

    override fun createTask(id: Long, type: String, action: String, parentTaskId: TaskId,
                            taskInProgress: PersistentTask<AutoFollowParams>,
                            headers: Map<String, String>): AllocatedPersistentTask {
        return AutoFollowTask(id, type, action, getDescription(taskInProgress), parentTaskId, headers,
                              executor, clusterService, threadPool, client, taskInProgress.params!!)
    }

    override fun getDescription(taskInProgress: PersistentTask<AutoFollowParams>): String {
        return "replication auto follow task for remote cluster: ${taskInProgress.params?.remoteCluster} with pattern " +
                "${taskInProgress.params?.patternName}"
    }
}