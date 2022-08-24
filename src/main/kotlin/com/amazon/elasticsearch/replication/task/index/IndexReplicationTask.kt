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

import com.amazon.elasticsearch.replication.ReplicationException
import com.amazon.elasticsearch.replication.ReplicationPlugin.Companion.PLUGINS_REPLICATION_TRANSLOG_PRUNING_SETTING
import com.amazon.elasticsearch.replication.ReplicationPlugin.Companion.REPLICATED_INDEX_SETTING
import com.amazon.elasticsearch.replication.ReplicationSettings
import com.amazon.elasticsearch.replication.action.index.block.IndexBlockUpdateType
import com.amazon.elasticsearch.replication.action.index.block.UpdateIndexBlockAction
import com.amazon.elasticsearch.replication.action.index.block.UpdateIndexBlockRequest
import com.amazon.elasticsearch.replication.action.pause.PauseIndexReplicationAction
import com.amazon.elasticsearch.replication.action.pause.PauseIndexReplicationRequest
import com.amazon.elasticsearch.replication.metadata.ReplicationMetadataManager
import com.amazon.elasticsearch.replication.metadata.ReplicationOverallState
import com.amazon.elasticsearch.replication.metadata.UpdateMetadataAction
import com.amazon.elasticsearch.replication.metadata.UpdateMetadataRequest
import com.amazon.elasticsearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import com.amazon.elasticsearch.replication.metadata.state.getReplicationStateParamsForIndex
import com.amazon.elasticsearch.replication.repository.REMOTE_SNAPSHOT_NAME
import com.amazon.elasticsearch.replication.repository.RemoteClusterRepository
import com.amazon.elasticsearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import com.amazon.elasticsearch.replication.task.CrossClusterReplicationTask
import com.amazon.elasticsearch.replication.task.ReplicationState
import com.amazon.elasticsearch.replication.task.shard.ShardReplicationExecutor
import com.amazon.elasticsearch.replication.task.shard.ShardReplicationParams
import com.amazon.elasticsearch.replication.task.shard.ShardReplicationTask
import com.amazon.elasticsearch.replication.util.removeTask
import com.amazon.elasticsearch.replication.util.stackTraceToString
import com.amazon.elasticsearch.replication.util.startTask
import com.amazon.elasticsearch.replication.util.suspendExecute
import com.amazon.elasticsearch.replication.util.suspendExecuteWithRetries
import com.amazon.elasticsearch.replication.util.suspending
import com.amazon.elasticsearch.replication.util.waitForNextChange
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.ElasticsearchTimeoutException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.client.Client
import org.elasticsearch.client.Requests
import org.elasticsearch.cluster.ClusterChangedEvent
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.ClusterStateListener
import org.elasticsearch.cluster.ClusterStateObserver
import org.elasticsearch.cluster.RestoreInProgress
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.common.settings.Setting
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.settings.SettingsModule
import org.elasticsearch.common.unit.ByteSizeUnit
import org.elasticsearch.common.unit.ByteSizeValue
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.index.Index
import org.elasticsearch.index.IndexService
import org.elasticsearch.index.IndexSettings
import org.elasticsearch.index.shard.IndexShard
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.indices.cluster.IndicesClusterStateService
import org.elasticsearch.indices.recovery.RecoveryState
import org.elasticsearch.persistent.PersistentTaskState
import org.elasticsearch.persistent.PersistentTasksCustomMetadata
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask
import org.elasticsearch.persistent.PersistentTasksNodeService
import org.elasticsearch.persistent.PersistentTasksService
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.tasks.TaskId
import org.elasticsearch.threadpool.ThreadPool
import java.util.Collections
import java.util.function.Predicate
import java.util.stream.Collectors
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlin.streams.toList

class IndexReplicationTask(id: Long, type: String, action: String, description: String,
                           parentTask: TaskId,
                           executor: String,
                           clusterService: ClusterService,
                           threadPool: ThreadPool,
                           client: Client,
                           params: IndexReplicationParams,
                           private val persistentTasksService: PersistentTasksService,
                           replicationMetadataManager: ReplicationMetadataManager,
                           replicationSettings: ReplicationSettings,
                           val settingsModule: SettingsModule)
    : CrossClusterReplicationTask(id, type, action, description, parentTask, emptyMap(), executor,
                                  clusterService, threadPool, client, replicationMetadataManager, replicationSettings), ClusterStateListener
    {
    private lateinit var currentTaskState : IndexReplicationState
    private lateinit var followingTaskState : IndexReplicationState

    override val leaderAlias = params.leaderAlias

    private val remoteClient = client.getRemoteClusterClient(leaderAlias)
    val leaderIndex   = params.leaderIndex
    override val followerIndexName = params.followerIndexName

    override val log = Loggers.getLogger(javaClass, Index(params.followerIndexName, ClusterState.UNKNOWN_UUID))
    private val cso = ClusterStateObserver(clusterService, log, threadPool.threadContext)
    private val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(clusterService.clusterName.value(), remoteClient)
    private var shouldCallEvalMonitoring = true
    private var updateSettingsContinuousFailCount = 0
    private var updateAliasContinousFailCount = 0

    private var metadataUpdate :MetadataUpdate? = null
    private var metadataPoller: Job? = null
    companion object {
        val blSettings  : Set<Setting<*>> = setOf(
                IndexSettings.INDEX_PLUGINS_REPLICATION_TRANSLOG_RETENTION_LEASE_PRUNING_ENABLED_SETTING,
                IndexMetadata.INDEX_READ_ONLY_SETTING,
                IndexMetadata.INDEX_BLOCKS_READ_SETTING,
                IndexMetadata.INDEX_BLOCKS_WRITE_SETTING,
                IndexMetadata.INDEX_BLOCKS_METADATA_SETTING,
                IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING,
                EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING,
                EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING,
                IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING
        )

        val blockListedSettings :Set<String> = blSettings.stream().map { k -> k.key }.collect(Collectors.toSet())

        const val SLEEP_TIME_BETWEEN_POLL_MS = 5000L
        const val TASK_CANCELLATION_REASON = "Index replication task was cancelled by user"
    }


    override fun indicesOrShards(): List<Any> = listOf(followerIndexName)

    override suspend fun execute(scope: CoroutineScope, initialState: PersistentTaskState?) {
        checkNotNull(initialState) { "Missing initial state" }
        followingTaskState = FollowingState(emptyMap())
        currentTaskState = initialState as IndexReplicationState
        while (scope.isActive) {
            try {
                val newState = when (currentTaskState.state) {
                    ReplicationState.INIT -> {
                        addListenerToInterruptTask()
                        if (isResumed()) {
                            log.debug("Resuming tasks now.")
                            InitFollowState
                        } else {
                            setupAndStartRestore()
                        }
                    }
                    ReplicationState.RESTORING -> {
                        log.info("In restoring state for $followerIndexName")
                        waitForRestore()
                    }
                    ReplicationState.INIT_FOLLOW -> {
                        log.info("Starting shard tasks")
                        addIndexBlockForReplication()
                        startShardFollowTasks(emptyMap())
                    }
                    ReplicationState.FOLLOWING -> {
                        if (currentTaskState is FollowingState) {
                            followingTaskState = (currentTaskState as FollowingState)
                            shouldCallEvalMonitoring = false
                            MonitoringState
                        } else {
                            throw ReplicationException("Wrong state type: ${currentTaskState::class}")
                        }
                    }
                    ReplicationState.MONITORING -> {
                        var state = evalMonitoringState()
                        if (metadataPoller == null) {
                            metadataPoller = scope.launch {
                                pollForMetadata(this)
                            }
                        }

                        if (state !is MonitoringState) {
                            // Tasks need to be started
                            state
                        } else {
                            state = pollShardTaskStatus((followingTaskState as FollowingState).shardReplicationTasks)
                            followingTaskState = startMissingShardTasks((followingTaskState as FollowingState).shardReplicationTasks)
                            when (state) {
                                is MonitoringState -> {
                                    updateMetadata()
                                }
                                is FailedState -> {
                                    // Try pausing first if we get Failed state. This returns failed state if pause failed
                                    pauseReplication(state)
                                }
                                else -> {
                                    state
                                }
                            }
                        }
                    }
                    ReplicationState.FAILED -> {
                        assert(currentTaskState is FailedState)
                        failReplication(currentTaskState as FailedState)
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
            } catch(e: ReplicationException) {
                log.error("Exiting index replication task", e)
                throw e
            } catch(e: ElasticsearchException) {
                val status = e.status().status
                // Index replication task shouldn't exit before shard replication tasks
                // As long as shard replication tasks doesn't encounter any errors, Index task
                // should continue to poll and Any failure encoutered from shard task should
                // invoke state transition and exit
                if(status < 500 && status != RestStatus.TOO_MANY_REQUESTS.status) {
                    log.error("Exiting index replication task", e)
                    throw e
                }
                log.debug("Encountered transient error while running index replication task", e)
                delay(SLEEP_TIME_BETWEEN_POLL_MS)
            }
        }
    }

    override fun onCancelled() {
        log.info("Cancelling the index replication task.")
        client.execute(PauseIndexReplicationAction.INSTANCE,
            PauseIndexReplicationRequest(followerIndexName, TASK_CANCELLATION_REASON))
        super.onCancelled()
    }
    private suspend fun failReplication(failedState: FailedState) {
        withContext(NonCancellable) {
            val reason = failedState.errorMsg
            try {
                replicationMetadataManager.updateIndexReplicationState(
                    followerIndexName,
                    ReplicationOverallState.FAILED,
                    reason
                )
            } catch (e: Exception) {
                log.error("Encountered exception while marking IndexReplicationTask:$id as failed", e)
            } finally {
                // This is required to end the state-machine loop
                markAsFailed(RuntimeException(reason))
            }
        }
    }

    private fun addListenerToInterruptTask() {
        clusterService.addListener(this)
    }

    private suspend fun startMissingShardTasks(shardTasks: Map<ShardId, PersistentTask<ShardReplicationParams>>): IndexReplicationState {
        val persistentTasks = clusterService.state().metadata.custom<PersistentTasksCustomMetadata>(PersistentTasksCustomMetadata.TYPE)

        val runningShardTasks = persistentTasks.findTasks(ShardReplicationExecutor.TASK_NAME, Predicate { true }).stream()
                .map { task -> task.params as ShardReplicationParams }
                .collect(Collectors.toList())

        val runningTasksForCurrentIndex = shardTasks.filter { entry -> runningShardTasks.find { task -> task.followerShardId == entry.key } != null}
        val numMissingTasks = shardTasks.size - runningTasksForCurrentIndex.size
        if (numMissingTasks > 0) {
            log.info("Starting $numMissingTasks missing shard task(s)")
            return startShardFollowTasks(runningTasksForCurrentIndex)
        }
        return FollowingState(shardTasks)
    }

    private suspend fun pollShardTaskStatus(shardTasks: Map<ShardId, PersistentTask<ShardReplicationParams>>): IndexReplicationState {
        val failedShardTasks = findAllReplicationFailedShardTasks(followerIndexName, clusterService.state())
        if (failedShardTasks.isNotEmpty()) {
            log.info("Failed shard tasks - ", failedShardTasks)
            var msg = ""
            for ((shard, task) in failedShardTasks) {
                val taskState = task.state
                if (taskState is com.amazon.elasticsearch.replication.task.shard.FailedState) {
                    val exception: ElasticsearchException? = taskState.exception
                    msg += "[${shard} - ${exception?.javaClass?.name} - \"${exception?.message}\"], "
                } else {
                    msg += "[${shard} - \"Shard task killed or cancelled.\"], "
                }
            }
            return FailedState(failedShardTasks, msg)
        }
        delay(SLEEP_TIME_BETWEEN_POLL_MS)
        return MonitoringState
    }

    private fun isResumed(): Boolean {
        return clusterService.state().routingTable.hasIndex(followerIndexName)
    }

    private suspend fun evalMonitoringState():IndexReplicationState {
        // Handling for node crashes during Static Index Updates'
        // Makes sure follower index is open, shard tasks are running
        // Shard & Index Tasks has Close listeners
        //All ops are idempotent .

        //Only called once if task starts in MONITORING STATE 
        if (!shouldCallEvalMonitoring) {
            return MonitoringState
        }

        val updateRequest = UpdateMetadataRequest(followerIndexName, UpdateMetadataRequest.Type.OPEN, Requests.openIndexRequest(followerIndexName))
        client.suspendExecute(UpdateMetadataAction.INSTANCE, updateRequest, injectSecurityContext = true)

        registerCloseListeners()
        val clusterState = clusterService.state()
        val persistentTasks = clusterState.metadata.custom<PersistentTasksCustomMetadata>(PersistentTasksCustomMetadata.TYPE)
        val runningShardTasks = persistentTasks.findTasks(ShardReplicationExecutor.TASK_NAME, Predicate { true }).stream()
                .map { task -> task.params as ShardReplicationParams }
                .collect(Collectors.toList())

        if (runningShardTasks.size == 0) {
            return InitFollowState
        }

        shouldCallEvalMonitoring = false
        return MonitoringState
    }

    private suspend fun updateMetadata() :IndexReplicationState {
        val meta = metadataUpdate
        if (meta == null) {
            return MonitoringState
        }
        var needsInit = false
        var errorEncountered = false
        val shouldSettingsLogError = updateSettingsContinuousFailCount < 12 || ((updateSettingsContinuousFailCount % 12) == 0)
        val shouldAliasLogError = updateAliasContinousFailCount < 12  || ((updateAliasContinousFailCount % 12) == 0)

        try {
            updateAlias()
        } catch (e: Exception) {
            errorEncountered = true
            if (shouldAliasLogError) {
                log.error("Encountered exception while updating alias ${e.stackTraceToString()}")
            }
        } finally {
            if (errorEncountered) {
                ++updateAliasContinousFailCount
            } else {
                // reset counter
                updateAliasContinousFailCount = 0
            }
        }

        errorEncountered = false
        try {
            needsInit = updateSettings()
        } catch (e: Exception) {
            errorEncountered = true
            if (shouldSettingsLogError) {
                log.error("Encountered exception while updating settings ${e.stackTraceToString()}")
            }
        } finally {
            if (errorEncountered) {
                ++updateSettingsContinuousFailCount
                return InitFollowState
            } else {
                // reset counter
                updateSettingsContinuousFailCount = 0
            }
        }

        if (needsInit) {
            return InitFollowState
        } else {
            return MonitoringState
        }
    }

    private suspend fun pollForMetadata(scope: CoroutineScope) {
        while (scope.isActive) {
            try {
                log.debug("Polling for metadata for $followerIndexName")
                var staticUpdated = false
                var gsr = GetSettingsRequest().includeDefaults(false).indices(this.leaderIndex.name)
                var settingsResponse = remoteClient.suspending(remoteClient.admin().indices()::getSettings, injectSecurityContext = true)(gsr)
                //  There is no mechanism to retrieve settingsVersion from client
                // If we we want to retrieve just the version of settings and alias versions, there are two options
                // 1. Include this in GetChanges and communicate it to IndexTask via Metadata
                // 2. Add another API to retrieve version of settings & aliases. Persist current version in Metadata
                var leaderSettings = settingsResponse.indexToSettings.get(this.leaderIndex.name)
                leaderSettings = leaderSettings.filter { k: String? ->
                    !blockListedSettings.contains(k)
                }

                gsr = GetSettingsRequest().includeDefaults(false).indices(this.followerIndexName)
                settingsResponse = client.suspending(client.admin().indices()::getSettings, injectSecurityContext = true)(gsr)
                var followerSettings = settingsResponse.indexToSettings.get(this.followerIndexName)

                followerSettings = followerSettings.filter { k: String? ->
                    k != REPLICATED_INDEX_SETTING.key
                }

                val replMetdata = replicationMetadataManager.getIndexReplicationMetadata(this.followerIndexName)
                var overriddenSettings = replMetdata.settings

                val indexScopedSettings = settingsModule.indexScopedSettings

                val settingsList = arrayOf(leaderSettings, overriddenSettings)
                val desiredSettingsBuilder = Settings.builder()
                // Desired settings are taking leader Settings and then overriding them with desired settings
                for (settings in settingsList) {
                    for (key in settings.keySet()) {
                        if (indexScopedSettings.isPrivateSetting(key)) {
                            continue
                        }
                        val setting = indexScopedSettings[key]
                        if (!setting.isPrivateIndex) {
                            desiredSettingsBuilder.copy(key, settings);
                        }
                    }
                }
                val desiredSettings = desiredSettingsBuilder.build()

                val changedSettingsBuilder = Settings.builder()
                for(key in desiredSettings.keySet()) {
                    if (desiredSettings.get(key) != followerSettings.get(key)) {
                        //Not intended setting on follower side.
                        val setting = indexScopedSettings[key]
                        if (indexScopedSettings.isPrivateSetting(key)) {
                            continue
                        }
                        if (!setting.isDynamic()) {
                            staticUpdated = true
                        }
                        log.info("Adding setting $key for $followerIndexName")
                        changedSettingsBuilder.copy(key, desiredSettings);
                    }
                }

                for (key in followerSettings.keySet()) {
                    val setting = indexScopedSettings[key]
                    if (setting == null || setting.isPrivateIndex) {
                        continue
                    }

                    if (desiredSettings.get(key) == null) {
                        if (!setting.isDynamic()) {
                            staticUpdated = true
                        }

                        log.info("Removing setting $key from $followerIndexName")
                        changedSettingsBuilder.putNull(key)
                    }
                }

                val changedSettings = changedSettingsBuilder.build()

                var updateSettingsRequest :UpdateSettingsRequest?

                if (changedSettings.keySet().size == 0) {
                    log.debug("No settings to apply")
                    updateSettingsRequest = null
                } else {
                    updateSettingsRequest = Requests.updateSettingsRequest(followerIndexName)
                    updateSettingsRequest.settings(changedSettings)
                    log.info("Got index settings to apply ${changedSettings} for $followerIndexName")
                }

                //Alias
                var getAliasesRequest = GetAliasesRequest().indices(this.leaderIndex.name)
                var getAliasesRes = remoteClient.suspending(remoteClient.admin().indices()::getAliases, injectSecurityContext = true)(getAliasesRequest)
                var leaderAliases = getAliasesRes.aliases.get(this.leaderIndex.name)

                getAliasesRequest = GetAliasesRequest().indices(followerIndexName)
                getAliasesRes = client.suspending(client.admin().indices()::getAliases, injectSecurityContext = true)(getAliasesRequest)
                var followerAliases = getAliasesRes.aliases.get(followerIndexName)

                var request  :IndicesAliasesRequest?

                if (leaderAliases == followerAliases) {
                    log.debug("All aliases equal")
                    request = null
                } else {
                    log.info("All aliases are not equal on $followerIndexName. Will sync up them")
                    request  = IndicesAliasesRequest()
                    var toAdd = leaderAliases - followerAliases

                    for (alias in toAdd) {
                        log.info("Adding alias ${alias.alias} from $followerIndexName")
                        // Copying writeIndex from leader doesn't cause any issue as writes will be blocked anyways
                        var aliasAction = AliasActions.add().index(followerIndexName)
                            .alias(alias.alias)
                            .indexRouting(alias.indexRouting)
                            .searchRouting(alias.searchRouting)
                            .writeIndex(alias.writeIndex())
                            .isHidden(alias.isHidden)

                        if (alias.filteringRequired())  {
                            aliasAction = aliasAction.filter(alias.filter.string())
                        }

                        request.addAliasAction(aliasAction)
                    }

                    var toRemove = followerAliases - leaderAliases

                    for (alias in toRemove) {
                        log.info("Removing alias  ${alias.alias} from $followerIndexName")
                        request.addAliasAction(AliasActions.remove().index(followerIndexName)
                                .alias(alias.alias))
                    }
                }

                if (updateSettingsRequest != null || request != null) {
                    metadataUpdate = MetadataUpdate(updateSettingsRequest, request, staticUpdated)
                } else {
                    metadataUpdate = null
                }

            } catch (e: Exception) {
                log.error("Error in getting the required metadata ${e.stackTraceToString()}")
            } finally {
                withContext(NonCancellable) {
                    log.debug("Metadata sync sleeping for ${replicationSettings.metadataSyncInterval.millis}")
                    delay(replicationSettings.metadataSyncInterval.millis)
                }
            }
        }
    }

    private suspend fun updateSettings() :Boolean {
        var updateSettingsRequest = metadataUpdate!!.updateSettingsRequest
        var staticUpdated = metadataUpdate!!.staticUpdated

        if (updateSettingsRequest == null) {
            return false
        }

        log.info("Got index settings to apply ${updateSettingsRequest.settings()}")

        if (staticUpdated) {
            log.info("Handle static settings change ${updateSettingsRequest.settings()}")

            try {
                //Step 1 : Remove the tasks
                val shards = clusterService.state().routingTable.indicesRouting().get(followerIndexName).shards()
                shards.forEach {
                    persistentTasksService.removeTask(ShardReplicationTask.taskIdForShard(it.value.shardId))
                }

                //Step 2 : Unregister Close Listener w/o which the Index Task is going to get cancelled
                unregisterCloseListeners()

                //Step 3 : Close index
                log.info("Closing the index $followerIndexName to apply static settings now")
                var updateRequest = UpdateMetadataRequest(followerIndexName, UpdateMetadataRequest.Type.CLOSE, Requests.closeIndexRequest(followerIndexName))
                client.suspendExecute(UpdateMetadataAction.INSTANCE, updateRequest, injectSecurityContext = true)
                log.info("Closed the index $followerIndexName to apply static settings now")

                //Step 4 : apply settings
                updateRequest = UpdateMetadataRequest(followerIndexName, UpdateMetadataRequest.Type.SETTING, updateSettingsRequest)
                client.suspendExecute(UpdateMetadataAction.INSTANCE, updateRequest, injectSecurityContext = true)

            } finally {
                //Step 5: open the index
                val updateRequest = UpdateMetadataRequest(followerIndexName, UpdateMetadataRequest.Type.OPEN, Requests.openIndexRequest(followerIndexName))
                client.suspendExecute(UpdateMetadataAction.INSTANCE, updateRequest, injectSecurityContext = true)
                log.info("Opened the index $followerIndexName now post applying static settings")

                //Step 6 :  Register Close Listeners again
                registerCloseListeners()
            }

        } else {
            log.info("Handling dynamic settings change")
            val updateRequest = UpdateMetadataRequest(followerIndexName, UpdateMetadataRequest.Type.SETTING, updateSettingsRequest)
            try {
                client.suspendExecute(UpdateMetadataAction.INSTANCE, updateRequest, injectSecurityContext = true)
            } catch (e: Exception) {
                log.error("Got an error while updating dynamic settings ${followerIndexName} - ${e.stackTraceToString()} ")
            }
        }

        log.info("Updated settings for $followerIndexName")
        return staticUpdated
    }

    private suspend fun updateAlias() {
        val request = metadataUpdate!!.aliasReq
        if (request == null) {
            return
        }
        val updateRequest = UpdateMetadataRequest(followerIndexName, UpdateMetadataRequest.Type.ALIAS, request)
        client.suspendExecute(UpdateMetadataAction.INSTANCE, updateRequest, injectSecurityContext = true)
    }

    private suspend fun pauseReplication(state: FailedState): IndexReplicationState {
        try {
            log.info("Going to initiate auto-pause of $followerIndexName due to shard failures - $state")
            val pauseReplicationResponse = client.suspendExecute(
                replicationMetadata,
                PauseIndexReplicationAction.INSTANCE, PauseIndexReplicationRequest(followerIndexName, "AutoPaused: ${state.errorMsg}"),
                defaultContext = true
            )
            if (!pauseReplicationResponse.isAcknowledged) {
                throw ReplicationException(
                    "Failed to gracefully pause replication after one or more shard tasks failed. " +
                            "Replication tasks may need to be paused manually."
                )
            }
        } catch (e: CancellationException) {
            // If pause completed before and this co-routine was cancelled
            throw e
        } catch (e: Exception) {
            log.error("Encountered exception while auto-pausing $followerIndexName", e)
            return FailedState(state.failedShards,
                "Pause failed with \"${e.message}\". Original failure for initiating pause - ${state.errorMsg}")
        }
        return MonitoringState
    }

    private fun findAllReplicationFailedShardTasks(followerIndexName: String, clusterState: ClusterState)
            :Map<ShardId, PersistentTask<ShardReplicationParams>> {
        val persistentTasks = clusterState.metadata.custom<PersistentTasksCustomMetadata>(PersistentTasksCustomMetadata.TYPE)

        // Filter tasks related to the follower shard index and construct the error message
        val failedShardTasks = persistentTasks.findTasks(ShardReplicationExecutor.TASK_NAME, Predicate { true }).stream()
                .filter { task -> task.state is com.amazon.elasticsearch.replication.task.shard.FailedState }
                .map { task -> task as PersistentTask<ShardReplicationParams> }
                .filter { task -> task.params!!.followerShardId.indexName  == followerIndexName}
                .collect(Collectors.toMap(
                        {t: PersistentTask<ShardReplicationParams> -> t.params!!.followerShardId},
                        {t: PersistentTask<ShardReplicationParams> -> t}))

        return failedShardTasks
    }

    override suspend fun cleanup() {
        if (currentTaskState.state == ReplicationState.RESTORING)  {
            log.info("Replication stopped before restore could finish, so removing partial restore..")
            cancelRestore()
        }
        /* This is to minimise overhead of calling an additional listener as
         * it continues to be called even after the task is completed.
         */
        clusterService.removeListener(this)
    }

    private suspend fun addIndexBlockForReplication() {
        log.info("Adding index block for replication")
        val request = UpdateIndexBlockRequest(followerIndexName, IndexBlockUpdateType.ADD_BLOCK)
        client.suspendExecute(replicationMetadata, UpdateIndexBlockAction.INSTANCE, request, defaultContext = true)
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
                startReplicationTask(ShardReplicationParams(leaderAlias, ShardId(leaderIndex, shardId.id), shardId))
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
        val shards = clusterService.state().routingTable.indicesRouting().get(followerIndexName)?.shards()
        shards?.forEach {
            val followerShardId = it.value.shardId
            retentionLeaseHelper.attemptRetentionLeaseRemoval(ShardId(leaderIndex, followerShardId.id), followerShardId)
        }

        /* As given here
         * (https://www.elastic.co/guide/en/elasticsearch/reference/6.8/modules-snapshots.html#_stopping_currently_running_snapshot_and_restore_operations)
         * a snapshot restore can be cancelled by deleting the indices being restored.
         */
        log.info("Deleting the index $followerIndexName")
        client.suspending(client.admin().indices()::delete, defaultContext = true)(DeleteIndexRequest(followerIndexName))
    }

    private suspend fun setupAndStartRestore(): IndexReplicationState {
        // Enable translog based fetch on the leader(remote) cluster
        val remoteClient = client.getRemoteClusterClient(leaderAlias)

        // These settings enables some of the optimizations at the leader cluster side to retrive translog operations
        // INDEX_PLUGINS_REPLICATION_TRANSLOG_RETENTION_LEASE_PRUNING_ENABLED_SETTING - This setting enables pruning
        // of translog based on retention lease. So that, we can directly fetch the ops in decompressed form from translog
        // INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING - This setting sets each generation size for the translog.
        // This ensures that, we don't have to search the huge translog files for the given range and ensuring that
        // the searches are optimal within a generation and skip searching the generations based on translog checkpoints
        val settingsBuilder = Settings.builder()
                .put(IndexSettings.INDEX_PLUGINS_REPLICATION_TRANSLOG_RETENTION_LEASE_PRUNING_ENABLED_SETTING.key, true)
                .put(IndexSettings.INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING.key, ByteSizeValue(32, ByteSizeUnit.MB))
        val updateSettingsRequest = remoteClient.admin().indices().prepareUpdateSettings().setSettings(settingsBuilder).setIndices(leaderIndex.name).request()
        val updateResponse = remoteClient.suspending(remoteClient.admin().indices()::updateSettings, injectSecurityContext = true)(updateSettingsRequest)
        if(!updateResponse.isAcknowledged) {
            log.error("Unable to update setting for translog pruning based on retention lease")
        }

        val restoreRequest = client.admin().cluster()
            .prepareRestoreSnapshot(RemoteClusterRepository.repoForCluster(leaderAlias), REMOTE_SNAPSHOT_NAME)
            .setIndices(leaderIndex.name)
            .request()
        if (leaderIndex.name != followerIndexName) {
            restoreRequest.renamePattern(leaderIndex.name)
                .renameReplacement(followerIndexName)
        }
        val replMetadata = replicationMetadataManager.getIndexReplicationMetadata(this.followerIndexName)
        restoreRequest.indexSettings(replMetadata.settings)

        try {
            val response = client.suspending(client.admin().cluster()::restoreSnapshot, defaultContext = true)(restoreRequest)
            if (response.restoreInfo != null) {
                if (response.restoreInfo.failedShards() != 0) {
                    throw ReplicationException("Restore failed: $response")
                }
                return FollowingState(emptyMap())
            }
        } catch (e: Exception) {
            val err = "Unable to initiate restore call for $followerIndexName from $leaderAlias:${leaderIndex.name}"
            log.error(err, e)
            return FailedState(Collections.emptyMap(), err)
        }
        cso.waitForNextChange("remote restore start") { inProgressRestore(it) != null }
        return RestoreState
    }

    private suspend fun waitForRestore(): IndexReplicationState {
        var restore = inProgressRestore(clusterService.state())

        // Waiting for snapshot restore to reach a terminal stage.
        while (restore != null && restore.state() != RestoreInProgress.State.FAILURE && restore.state() != RestoreInProgress.State.SUCCESS) {
            try {
                cso.waitForNextChange("remote restore finish")
            } catch(e: ElasticsearchTimeoutException) {
                log.info("Timed out while waiting for restore to complete.")
            }
            restore = inProgressRestore(clusterService.state())
        }

        if (restore == null) {
            /**
             * At this point, we've already verified (during startRestore) that RestoreInProgress entry was
             * added in cluster state. Now if the entry is not present, we validate that no primary shard
             * is in recovery. Post validation we assume that restore is already completed and entry has been
             * cleaned up from cluster state.
             *
             * This we've observed when we trigger the replication on multiple small indices(also autofollow) simultaneously.
             */
            if (doesValidIndexExists()) {
                return InitFollowState
            } else {
                return FailedState(Collections.emptyMap(), """
                    Unable to find in progress restore for remote index: $leaderAlias:$leaderIndex. 
                    This can happen if there was a badly timed master node failure.""".trimIndent())
            }
        } else if (restore.state() == RestoreInProgress.State.FAILURE) {
            val failureReason = restore.shards().values().find {
                it.value.state() == RestoreInProgress.State.FAILURE
            }!!.value.reason()
            return FailedState(Collections.emptyMap(), failureReason)
        } else {
            return InitFollowState
        }
    }

    /**
     * In case the snapshot entry is not present in the cluster state, we assume that the task has
     * been successful and has been cleaned up from cluster state. With this method, we do basic
     * validation for the index before we allow the index replication to move to following state.
     * The validation done are:
     * 1. The index still exists and has been created using replication
     *    workflow i.e. index settings contains 'index.plugins.replication.follower.leader_index'
     * 2. There shouldn't be any primary shard in active recovery.
     */
    private fun doesValidIndexExists(): Boolean {
        try {
            client.admin().indices().prepareGetSettings(followerIndexName).get()
                    .getSetting(followerIndexName, REPLICATED_INDEX_SETTING.key) ?: return false

            val recoveries = client.admin().indices().prepareRecoveries(followerIndexName).get()
                .shardRecoveryStates().get(followerIndexName)
            val activeRecoveries = recoveries?.stream()?.filter(RecoveryState::getPrimary)?.filter {
                    r -> r.stage != RecoveryState.Stage.DONE }?.toList()
            return activeRecoveries?.size == 0
        } catch (e: Exception) {
            log.error("Error trying to validate the index", e)
            return false
        }
    }
    private fun inProgressRestore(cs: ClusterState): RestoreInProgress.Entry? {
        return cs.custom<RestoreInProgress>(RestoreInProgress.TYPE).singleOrNull { entry ->
            entry.snapshot().repository == RemoteClusterRepository.repoForCluster(leaderAlias) &&
                entry.indices().singleOrNull { idx -> idx == followerIndexName } != null
        }
    }

    private suspend
    fun startReplicationTask(replicationParams : ShardReplicationParams) : PersistentTask<ShardReplicationParams> {
        return persistentTasksService.startTask(ShardReplicationTask.taskIdForShard(replicationParams.followerShardId),
            ShardReplicationExecutor.TASK_NAME, replicationParams)
    }

    override fun onIndexShardClosed(shardId: ShardId, indexShard: IndexShard?, indexSettings: Settings) {
        // Index task shouldn't cancel if the shards are moved within the cluster.
        // Currently, we don't have any actions to trigger based on this event
    }

    override fun onIndexRemoved(indexService: IndexService,
                                reason: IndicesClusterStateService.AllocatedIndices.IndexRemovalReason) {
        // cancel the index task only if the index is closed
        val indexMetadata = indexService.indexSettings.indexMetadata
        log.debug("onIndexRemoved called")
        if(indexMetadata.state != IndexMetadata.State.OPEN) {
            log.info("onIndexRemoved cancelling the task")
            cancelTask("${indexService.index().name} was closed.")
        }
    }

    override fun clusterChanged(event: ClusterChangedEvent) {
        log.debug("Cluster metadata listener invoked on index task...")
        if (event.metadataChanged()) {
            val replicationStateParams = getReplicationStateParamsForIndex(clusterService, followerIndexName)
            log.debug("$replicationStateParams from the cluster state")
            if (replicationStateParams == null) {
                if (PersistentTasksNodeService.Status(State.STARTED) == status) {
                    log.debug("Cancelling index replication stop")
                    cancelTask("Index replication task received an interrupt.")
                }
            } else if (replicationStateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE] == ReplicationOverallState.PAUSED.name){
                log.info("Pause state received for index $followerIndexName task")
                cancelTask("Index replication task received a pause.")
            }
        }
    }

    override suspend fun setReplicationMetadata() {
        this.replicationMetadata = replicationMetadataManager.getIndexReplicationMetadata(followerIndexName, fetch_from_primary = true)
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

    data class MetadataUpdate(val updateSettingsRequest: UpdateSettingsRequest?, val aliasReq: IndicesAliasesRequest?, val staticUpdated: Boolean) {

    }

}
