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

import com.nhaarman.mockitokotlin2.doReturn
import org.mockito.Mockito
import org.opensearch.core.action.ActionListener
import org.opensearch.action.ActionRequest
import org.opensearch.core.action.ActionResponse
import org.opensearch.action.ActionType
import org.opensearch.action.admin.cluster.health.ClusterHealthAction
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse
import org.opensearch.action.admin.indices.recovery.RecoveryAction
import org.opensearch.action.admin.indices.recovery.RecoveryResponse
import org.opensearch.action.admin.indices.settings.get.GetSettingsAction
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsAction
import org.opensearch.action.get.GetAction
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.common.UUIDs
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.common.settings.Settings
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.core.index.Index
import org.opensearch.index.get.GetResult
import org.opensearch.core.index.shard.ShardId
import org.opensearch.indices.recovery.RecoveryState
import org.opensearch.persistent.PersistentTaskResponse
import org.opensearch.persistent.PersistentTasksCustomMetadata
import org.opensearch.persistent.StartPersistentTaskAction
import org.opensearch.persistent.UpdatePersistentTaskStatusAction
import org.opensearch.replication.ReplicationPlugin
import org.opensearch.replication.action.index.block.UpdateIndexBlockAction
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.metadata.store.ReplicationContext
import org.opensearch.replication.metadata.store.ReplicationMetadata
import org.opensearch.replication.metadata.store.ReplicationMetadataStore
import org.opensearch.replication.metadata.store.ReplicationStoreMetadataType
import org.opensearch.replication.task.shard.ShardReplicationExecutor
import org.opensearch.replication.task.shard.ShardReplicationParams
import org.opensearch.snapshots.RestoreInfo
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.client.NoOpNodeClient
import java.lang.reflect.Field
import java.util.*

open class NoOpClient(testName :String) : NoOpNodeClient(testName) {
    @Override
    override fun <Request : ActionRequest, Response : ActionResponse> doExecute(action: ActionType<Response>?, request: Request?, listener: ActionListener<Response>) {
        if (action == UpdateSettingsAction.INSTANCE) {
            //Update setting to prevent pruning on leader
            var settingResponse = AcknowledgedResponse(true)
            listener.onResponse(settingResponse as Response)
        } else if (action == RestoreSnapshotAction.INSTANCE) {
            //begin snapshot operation
            var snapResponse = RestoreSnapshotResponse(null as RestoreInfo?)
            if (IndexReplicationTaskTests.restoreNotNull) {
                snapResponse = RestoreSnapshotResponse(RestoreInfo("name", emptyList(), 1, 1))
            }
            listener.onResponse(snapResponse as Response)
        } else if (action == UpdatePersistentTaskStatusAction.INSTANCE) {
            // update status of index replication task
            var r = request as UpdatePersistentTaskStatusAction.Request
            val obj: Class<*> = r.javaClass
            // access the private variable "state"
            val field: Field = obj.getDeclaredField("state")
            field.setAccessible(true)
            val taskState = field.get(r) as IndexReplicationState

            IndexReplicationTaskTests.currentTaskState = taskState
            IndexReplicationTaskTests.stateChanges++

            var t = Mockito.mock(PersistentTasksCustomMetadata.PersistentTask::class.java)
            var t1 = Mockito.mock(PersistentTaskResponse::class.java)
            doReturn(t).`when`(t1).task
            doReturn(taskState).`when`(t).getState()
            //var settingResponse = PersistentTaskResponse(true)
            listener.onResponse(t1 as Response)
        } else if (action == UpdateIndexBlockAction.INSTANCE) {
            // applies index block
            var settingResponse = AcknowledgedResponse(true)
            listener.onResponse(settingResponse as Response)
        }  else if (action == StartPersistentTaskAction.INSTANCE) {
            var sId = ShardId(Index(IndexReplicationTaskTests.followerIndex, "_na_"), 0)
            var t1 = PersistentTaskResponse(
                    PersistentTasksCustomMetadata.PersistentTask<ShardReplicationParams>(UUIDs.base64UUID(), ShardReplicationExecutor.TASK_NAME,
                            ShardReplicationParams(IndexReplicationTaskTests.remoteCluster, sId, sId),
                            OpenSearchTestCase.randomLong(), PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT))

            listener.onResponse(t1 as Response)
        } else if (action == GetSettingsAction.INSTANCE) {
            //called in doesValidIndexExists after restore is complete
            val desiredSettingsBuilder = Settings.builder()
            desiredSettingsBuilder.put(ReplicationPlugin.REPLICATED_INDEX_SETTING.key, "true")

            val indexToSettings = HashMap<String, Settings>()
            indexToSettings[IndexReplicationTaskTests.followerIndex] =  desiredSettingsBuilder.build()
            var settingResponse = GetSettingsResponse(indexToSettings, indexToSettings)
            listener.onResponse(settingResponse as Response)
        } else if (action == RecoveryAction.INSTANCE) {
            val shardRecoveryStates: MutableMap<String, List<RecoveryState>> = HashMap()
            val recoveryStates: MutableList<RecoveryState> = ArrayList()
            recoveryStates.add(Mockito.mock(RecoveryState::class.java))
            shardRecoveryStates.put("follower-index", recoveryStates)
            var recoveryResponse = RecoveryResponse(1,1, 1, shardRecoveryStates, listOf())
            listener.onResponse(recoveryResponse as Response)
        } else if (action == GetAction.INSTANCE) {
            // Replication Metadata store
            val replicationMetadata = ReplicationMetadata(IndexReplicationTaskTests.connectionName,
                    ReplicationStoreMetadataType.INDEX.name, "overallState.name", ReplicationMetadataManager.CUSTOMER_INITIATED_ACTION,
                    ReplicationContext(IndexReplicationTaskTests.followerIndex, null),
                    ReplicationContext(IndexReplicationTaskTests.followerIndex, null), Settings.EMPTY)

            var bytesReference  = replicationMetadata.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)
            var by = BytesReference.bytes(bytesReference)
            var result = GetResult(ReplicationMetadataStore.REPLICATION_CONFIG_SYSTEM_INDEX, IndexReplicationTaskTests.followerIndex, 1, 1, 1, true, by, null, null)
            var getResponse = GetResponse(result)
            listener.onResponse(getResponse as Response)
        } else if (action == ClusterHealthAction.INSTANCE) {
            // Store health response
            val replicationStoreResponse = ClusterHealthResponse()
            listener.onResponse(replicationStoreResponse as Response)
        }
    }
}
