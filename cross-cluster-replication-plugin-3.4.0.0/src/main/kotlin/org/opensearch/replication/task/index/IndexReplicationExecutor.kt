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

package org.opensearch.replication.task.index

import org.opensearch.replication.ReplicationSettings
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.metadata.ReplicationOverallState
import org.opensearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import org.opensearch.replication.metadata.state.getReplicationStateParamsForIndex
import org.opensearch.replication.util.persistentTasksService
import org.apache.logging.log4j.LogManager
import org.opensearch.transport.client.Client
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.ClusterStateObserver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.SettingsModule
import org.opensearch.persistent.AllocatedPersistentTask
import org.opensearch.persistent.PersistentTaskState
import org.opensearch.persistent.PersistentTasksCustomMetadata.PersistentTask
import org.opensearch.persistent.PersistentTasksExecutor
import org.opensearch.core.tasks.TaskId
import org.opensearch.threadpool.ThreadPool

class IndexReplicationExecutor(executor: String, private val clusterService: ClusterService,
                               private val threadPool: ThreadPool, private val client: Client,
                               private val replicationMetadataManager: ReplicationMetadataManager,
                               private val replicationSettings: ReplicationSettings,
                               var settingsModule: SettingsModule)
    : PersistentTasksExecutor<IndexReplicationParams>(TASK_NAME, executor) {

    companion object {
        const val TASK_NAME = "cluster:indices/admin/replication"
        val INITIAL_STATE = InitialState
        val log = LogManager.getLogger(IndexReplicationExecutor::class.java)
    }

    override fun validate(params: IndexReplicationParams, clusterState: ClusterState) {
        val replicationStateParams = getReplicationStateParamsForIndex(clusterService, params.followerIndexName)
                ?:
                throw IllegalStateException("Index task started without replication state in cluster metadata")
        if (replicationStateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE] != ReplicationOverallState.RUNNING.name) {
            throw IllegalArgumentException("Replication state for index:${params.followerIndexName} should be RUNNING, " +
                    "but was:${replicationStateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE]}")
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
        val cso = ClusterStateObserver(clusterService, log, threadPool.threadContext)
        return IndexReplicationTask(id, type, action, getDescription(taskInProgress), parentTaskId,
                                    executor, clusterService, threadPool, client, requireNotNull(taskInProgress.params),
                                    persistentTasksService, replicationMetadataManager, replicationSettings, settingsModule, cso)
    }

    override fun getDescription(taskInProgress: PersistentTask<IndexReplicationParams>): String {
        val params = requireNotNull(taskInProgress.params)
        return "replication:${params.leaderAlias}:${params.leaderIndex} -> ${params.followerIndexName}"
    }
}