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

import org.opensearch.replication.ReplicationPlugin.Companion.REPLICATED_INDEX_SETTING
import org.opensearch.replication.ReplicationSettings
import org.opensearch.replication.action.index.block.IndexBlockUpdateType
import org.opensearch.replication.action.index.block.UpdateIndexBlockAction
import org.opensearch.replication.action.index.block.UpdateIndexBlockRequest
import org.opensearch.replication.action.pause.PauseIndexReplicationAction
import org.opensearch.replication.action.pause.PauseIndexReplicationRequest
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.metadata.ReplicationOverallState
import org.opensearch.replication.metadata.UpdateMetadataAction
import org.opensearch.replication.metadata.UpdateMetadataRequest
import org.opensearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import org.opensearch.replication.metadata.state.getReplicationStateParamsForIndex
import org.opensearch.replication.repository.REMOTE_SNAPSHOT_NAME
import org.opensearch.replication.repository.RemoteClusterRepository
import org.opensearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import org.opensearch.replication.task.CrossClusterReplicationTask
import org.opensearch.replication.task.ReplicationState
import org.opensearch.replication.task.shard.ShardReplicationExecutor
import org.opensearch.replication.task.shard.ShardReplicationParams
import org.opensearch.replication.task.shard.ShardReplicationTask
import org.opensearch.replication.util.removeTask
import org.opensearch.replication.util.stackTraceToString
import org.opensearch.replication.util.startTask
import org.opensearch.replication.util.suspendExecute
import org.opensearch.replication.util.suspending
import org.opensearch.replication.util.waitForNextChange
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.opensearch.OpenSearchException
import org.opensearch.OpenSearchTimeoutException
import org.opensearch.core.action.ActionListener
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest
import org.opensearch.action.support.IndicesOptions
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.Requests
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.ClusterStateObserver
import org.opensearch.cluster.RestoreInProgress
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider
import org.opensearch.cluster.service.ClusterService
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.common.logging.Loggers
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.common.settings.SettingsModule
import org.opensearch.core.common.unit.ByteSizeUnit
import org.opensearch.core.common.unit.ByteSizeValue
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.index.Index
import org.opensearch.index.IndexService
import org.opensearch.index.IndexSettings
import org.opensearch.index.shard.IndexShard
import org.opensearch.core.index.shard.ShardId
import org.opensearch.indices.cluster.IndicesClusterStateService
import org.opensearch.indices.recovery.RecoveryState
import org.opensearch.persistent.PersistentTaskState
import org.opensearch.persistent.PersistentTasksCustomMetadata
import org.opensearch.persistent.PersistentTasksCustomMetadata.PersistentTask
import org.opensearch.persistent.PersistentTasksNodeService
import org.opensearch.persistent.PersistentTasksService
import org.opensearch.replication.ReplicationException
import org.opensearch.replication.MappingNotAvailableException
import org.opensearch.replication.ReplicationPlugin.Companion.REPLICATION_INDEX_TRANSLOG_PRUNING_ENABLED_SETTING
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.tasks.TaskId
import org.opensearch.tasks.TaskManager
import org.opensearch.threadpool.ThreadPool
import java.util.Collections
import java.util.function.Predicate
import java.util.stream.Collectors
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlin.streams.toList
import org.opensearch.cluster.DiffableUtils

open class IndexReplicationTask(id: Long, type: String, action: String, description: String,
                           parentTask: TaskId,
                           executor: String,
                           clusterService: ClusterService,
                           threadPool: ThreadPool,
                           client: Client,
                           params: IndexReplicationParams,
                           private val persistentTasksService: PersistentTasksService,
                           replicationMetadataManager: ReplicationMetadataManager,
                           replicationSettings: ReplicationSettings,
                           val settingsModule: SettingsModule,
                           val cso: ClusterStateObserver)
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
    private val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(clusterService.clusterName.value(), clusterService.state().metadata.clusterUUID(), remoteClient)
    private var shouldCallEvalMonitoring = true
    private var updateSettingsContinuousFailCount = 0
    private var updateAliasContinousFailCount = 0

    private var metadataUpdate :MetadataUpdate? = null
    private var metadataPoller: Job? = null
    companion object {
        val blSettings  : Set<Setting<*>> = setOf(
                REPLICATION_INDEX_TRANSLOG_PRUNING_ENABLED_SETTING,
                IndexMetadata.INDEX_READ_ONLY_SETTING,
                IndexMetadata.INDEX_BLOCKS_READ_SETTING,
                IndexMetadata.INDEX_BLOCKS_WRITE_SETTING,
                IndexMetadata.INDEX_BLOCKS_METADATA_SETTING,
                IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING,
                EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING,
                EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING,
                IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING,
                IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS
        )

        val blockListedSettings :Set<String> = blSettings.stream().map { k -> k.key }.collect(Collectors.toSet())

        const val SLEEP_TIME_BETWEEN_POLL_MS = 5000L
        const val AUTOPAUSED_REASON_PREFIX = "AutoPaused: "
        const val TASK_CANCELLATION_REASON = AUTOPAUSED_REASON_PREFIX + "Index replication task was cancelled by user"

    }

    //only for testing
    fun setPersistent(taskManager: TaskManager) {
        super.init(persistentTasksService, taskManager, "persistentTaskId", allocationId)
    }

    override fun indicesOrShards(): List<Any> = listOf(followerIndexName)

    public override suspend fun execute(scope: CoroutineScope, initialState: PersistentTaskState?) {
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
                        FollowingState(startNewOrMissingShardTasks())

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
                            state = pollShardTaskStatus()
                            followingTaskState = FollowingState(startNewOrMissingShardTasks())
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
            } catch(e: OpenSearchException) {
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

    private suspend fun failReplication(failedState: FailedState) {
        withContext(NonCancellable) {
            val reason = failedState.errorMsg
            log.error("Moving replication[IndexReplicationTask:$id][reason=${reason}] to failed state")
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

    private suspend fun pollShardTaskStatus(): IndexReplicationState {
        val failedShardTasks = findAllReplicationFailedShardTasks(followerIndexName, clusterService.state())
        if (failedShardTasks.isNotEmpty()) {
            log.info("Failed shard tasks - $failedShardTasks")
            var msg = ""
            for ((shard, task) in failedShardTasks) {
                val taskState = task.state
                if (taskState is org.opensearch.replication.task.shard.FailedState) {
                    val exception: OpenSearchException? = taskState.exception
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

    fun isTrackingTaskForIndex(): Boolean {
        val persistentTasks = clusterService.state().metadata.custom<PersistentTasksCustomMetadata>(PersistentTasksCustomMetadata.TYPE)
        val runningTasksForIndex = persistentTasks.findTasks(IndexReplicationExecutor.TASK_NAME, Predicate { true }).stream()
                .map { task -> task as PersistentTask<IndexReplicationParams> }
                .filter { task -> task.params!!.followerIndexName  == followerIndexName}
                .toArray()
        assert(runningTasksForIndex.size <= 1) { "Found more than one running index task for index[$followerIndexName]" }
        for (runningTask in runningTasksForIndex) {
            val currentTask = runningTask as PersistentTask<IndexReplicationParams>
            log.info("Verifying task details - currentTask={isAssigned=${currentTask.isAssigned},executorNode=${currentTask.executorNode}}")
            if(currentTask.isAssigned && currentTask.executorNode == clusterService.state().nodes.localNodeId) {
                return true
            }
        }
        return false
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

        val followerShardIds = clusterService.state().routingTable.indicesRouting().get(followerIndexName)?.shards()
            ?.map { shard -> shard.value.shardId }
            ?.stream()?.collect(Collectors.toSet()).orEmpty()
        val runningShardTasksForIndex = persistentTasks.findTasks(ShardReplicationExecutor.TASK_NAME, Predicate { true }).stream()
                .map { task -> task.params as ShardReplicationParams }
                .filter {taskParam -> followerShardIds.contains(taskParam.followerShardId) }
                .collect(Collectors.toList())

        if (runningShardTasksForIndex.size != followerShardIds.size) {
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

    private suspend fun updateFollowerMapping(followerIndex: String,mappingSource: String?) {

        val options = IndicesOptions.strictSingleIndexNoExpandForbidClosed()
        val putMappingRequest = PutMappingRequest().indices(followerIndex).indicesOptions(options)
            .source(mappingSource, XContentType.JSON)
        val updateMappingRequest = UpdateMetadataRequest(followerIndex, UpdateMetadataRequest.Type.MAPPING, putMappingRequest)
        client.suspendExecute(UpdateMetadataAction.INSTANCE, updateMappingRequest, injectSecurityContext = true)
        log.debug("Mappings synced for $followerIndex")
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
                var leaderSettings = settingsResponse.indexToSettings.getOrDefault(this.leaderIndex.name, Settings.EMPTY)
                leaderSettings = leaderSettings.filter { k: String ->
                    !blockListedSettings.contains(k)
                }

                gsr = GetSettingsRequest().includeDefaults(false).indices(this.followerIndexName)
                settingsResponse = client.suspending(client.admin().indices()::getSettings, injectSecurityContext = true)(gsr)
                var followerSettings = settingsResponse.indexToSettings.getOrDefault(this.followerIndexName, Settings.EMPTY)

                followerSettings = followerSettings.filter { k: String ->
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
                        if (!setting.isPrivateIndex && !setting.isFinal) {
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
                        if (indexScopedSettings.isPrivateSetting(key) || setting.isFinal) {
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
                    if (setting == null || setting.isPrivateIndex || setting.isFinal) {
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
                var leaderAliases = getAliasesRes.aliases.getOrDefault(this.leaderIndex.name, Collections.emptyList())

                getAliasesRequest = GetAliasesRequest().indices(followerIndexName)
                getAliasesRes = client.suspending(client.admin().indices()::getAliases, injectSecurityContext = true)(getAliasesRequest)
                var followerAliases = getAliasesRes.aliases.getOrDefault(followerIndexName, Collections.emptyList())

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
                val options = IndicesOptions.strictSingleIndexNoExpandForbidClosed()
                var gmr = GetMappingsRequest().indices(this.leaderIndex.name).indicesOptions(options)
                var mappingResponse = remoteClient.suspending(remoteClient.admin().indices()::getMappings, injectSecurityContext = true)(gmr)
                var leaderMappingSource = mappingResponse?.mappings?.get(this.leaderIndex.name)?.source()?.toString()
                @Suppress("UNCHECKED_CAST")
                val leaderProperties = mappingResponse?.mappings()?.get(this.leaderIndex.name)?.sourceAsMap()?.toMap()?.get("properties") as? Map<String,Any>?
                gmr = GetMappingsRequest().indices(this.followerIndexName).indicesOptions(options)
                mappingResponse = client.suspending(client.admin().indices()::getMappings, injectSecurityContext = true)(gmr)
                @Suppress("UNCHECKED_CAST")
                val followerProperties = mappingResponse?.mappings()?.get(this.followerIndexName)?.sourceAsMap()?.toMap()?.get("properties") as? Map<String,Any>?
                for((key,value) in followerProperties?: emptyMap()) {
                    if (leaderProperties?.getValue(key).toString() != (value).toString()) {
                        log.debug("Updating Multi-field Mapping at Follower")
                        updateFollowerMapping(this.followerIndexName, leaderMappingSource)
                        break
                    }
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
                val shards = clusterService.state().routingTable.indicesRouting().get(followerIndexName)?.shards()
                shards?.forEach {
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
            log.error("Going to initiate auto-pause of $followerIndexName due to shard failures - $state")
            val pauseReplicationResponse = client.suspendExecute(
                replicationMetadata,
                PauseIndexReplicationAction.INSTANCE, PauseIndexReplicationRequest(followerIndexName, "$AUTOPAUSED_REASON_PREFIX + ${state.errorMsg}"),
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
                .filter { task -> task.state is org.opensearch.replication.task.shard.FailedState }
                .map { task -> task as PersistentTask<ShardReplicationParams> }
                .filter { task -> task.params!!.followerShardId.indexName  == followerIndexName}
                .collect(Collectors.toMap(
                        {t: PersistentTask<ShardReplicationParams> -> t.params!!.followerShardId},
                        {t: PersistentTask<ShardReplicationParams> -> t}))

        return failedShardTasks
    }

    override suspend fun cleanup() {
        // If the task is already running on the other node,
        // OpenSearch persistent task framework cancels any stale tasks on the old nodes.
        // Currently, we don't have view on the cancellation reason. Before triggering
        // any further actions on the index from this task, verify that, this is the actual task tracking the index.
        // - stale task during cancellation shouldn't trigger further actions.
        if(isTrackingTaskForIndex()) {
            if (currentTaskState.state == ReplicationState.RESTORING)  {
                log.info("Replication stopped before restore could finish, so removing partial restore..")
                cancelRestore()
            }

            // if cancelled and not in paused state.
            val replicationStateParams = getReplicationStateParamsForIndex(clusterService, followerIndexName)
            if(isCancelled && replicationStateParams != null
                    && replicationStateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE] == ReplicationOverallState.RUNNING.name) {
                log.info("Task is cancelled. Moving the index to auto-pause state")
                client.execute(PauseIndexReplicationAction.INSTANCE,
                        PauseIndexReplicationRequest(followerIndexName, TASK_CANCELLATION_REASON))
            }
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

    suspend fun startNewOrMissingShardTasks():  Map<ShardId, PersistentTask<ShardReplicationParams>> {
        assert(clusterService.state().routingTable.hasIndex(followerIndexName)) { "Can't find index $followerIndexName" }
        val shards = clusterService.state().routingTable.indicesRouting().get(followerIndexName)?.shards()
        val persistentTasks = clusterService.state().metadata.custom<PersistentTasksCustomMetadata>(PersistentTasksCustomMetadata.TYPE)
        val runningShardTasks = persistentTasks.findTasks(ShardReplicationExecutor.TASK_NAME, Predicate { true }).stream()
            .map { task -> task as PersistentTask<ShardReplicationParams> }
            .filter { task -> task.params!!.followerShardId.indexName  == followerIndexName}
            .collect(Collectors.toMap(
                {t: PersistentTask<ShardReplicationParams> -> t.params!!.followerShardId},
                {t: PersistentTask<ShardReplicationParams> -> t}))

        val tasks = shards?.map {
            it.value.shardId
        }?.associate { shardId ->
            val task = runningShardTasks.getOrElse(shardId) {
                startReplicationTask(ShardReplicationParams(leaderAlias, ShardId(leaderIndex, shardId.id), shardId))
            }
            return@associate shardId to task
        }.orEmpty()

        return tasks
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
                .put(REPLICATION_INDEX_TRANSLOG_PRUNING_ENABLED_SETTING.key, true)
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
        } catch(e: Exception) {
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
            } catch(e: OpenSearchTimeoutException) {
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
                    This can happen if there was a badly timed cluster manager node failure.""".trimIndent())
            }
        } else if (restore.state() == RestoreInProgress.State.FAILURE) {
            val failureReason = restore.shards().values.find {
                it.state() == RestoreInProgress.State.FAILURE
            }!!.reason()
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
                    log.info("Cancelling index replication stop")
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
