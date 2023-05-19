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

package org.opensearch.replication.task.bulk

import org.opensearch.replication.ReplicationSettings
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.persistent.AllocatedPersistentTask
import org.opensearch.persistent.PersistentTaskState
import org.opensearch.persistent.PersistentTasksCustomMetadata.PersistentTask
import org.opensearch.persistent.PersistentTasksExecutor
import org.opensearch.replication.util.persistentTasksService
import org.opensearch.replication.util.removeTask
import org.opensearch.tasks.TaskId
import org.opensearch.threadpool.ThreadPool

class BulkExecuter(executor: String, private val clusterService: ClusterService,
                   private val threadPool: ThreadPool, private val client: Client,
                   private val replicationMetadataManager: ReplicationMetadataManager,
                   private val replicationSettings: ReplicationSettings,
                    private  val indexPattern: String) :
    PersistentTasksExecutor<BulkParams>(TASK_NAME, executor) {

    companion object {
        const val TASK_NAME = "cluster:admin/plugins/replication/autofollow"
    }

    override fun nodeOperation(task: AllocatedPersistentTask, params: BulkParams, state: PersistentTaskState?) {
        if (task is BulkCrossClusterReplicationTask) {
            task.run()
        } else {
            task.markAsFailed(IllegalArgumentException("unknown task type : ${task::class.java}"))
        }
    }

    override fun createTask(id: Long, type: String, action: String, parentTaskId: TaskId,
                            taskInProgress: PersistentTask<BulkParams>,
                            headers: Map<String, String>): AllocatedPersistentTask {

       val myBulkCrossClusterReplicationTask = BulkCrossClusterReplicationTask(id, type, action, getDescription(taskInProgress),
       parentTaskId, headers, threadPool, executor, clusterService, client,taskInProgress.params!! )
        return myBulkCrossClusterReplicationTask
    }

    suspend fun cancelMyTask(){
        persistentTasksService.removeTask("test")
    }

    override fun getDescription(taskInProgress: PersistentTask<BulkParams>): String {
        return "replication auto follow task for leader cluster:  with pattern " +
                "${taskInProgress.params?.patternName}"
    }
}