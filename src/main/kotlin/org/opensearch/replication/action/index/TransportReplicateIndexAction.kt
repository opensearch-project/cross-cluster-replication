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

package org.opensearch.replication.action.index

import org.opensearch.replication.ReplicationPlugin
import org.opensearch.replication.action.setup.SetupChecksAction
import org.opensearch.replication.action.setup.SetupChecksRequest
import org.opensearch.replication.metadata.store.ReplicationContext
import org.opensearch.replication.util.SecurityContext
import org.opensearch.replication.util.ValidationUtil
import org.opensearch.replication.util.completeWith
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.overrideFgacRole
import org.opensearch.replication.util.suspendExecute
import org.opensearch.replication.util.suspending
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.core.action.ActionListener
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.IndicesOptions
import org.opensearch.transport.client.Client
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.MetadataCreateIndexService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.cluster.service.ClusterService
import org.opensearch.env.Environment
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.IndexSettings
import org.opensearch.tasks.Task
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService

class TransportReplicateIndexAction @Inject constructor(transportService: TransportService,
                                                        private val clusterService: ClusterService,
                                                        val threadPool: ThreadPool,
                                                        actionFilters: ActionFilters,
                                                        private val client : Client,
                                                        private val environment: Environment,
                                                        private val metadataCreateIndexService: MetadataCreateIndexService) :
        HandledTransportAction<ReplicateIndexRequest, ReplicateIndexResponse>(ReplicateIndexAction.NAME,
                transportService, actionFilters, ::ReplicateIndexRequest),
    CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportReplicateIndexAction::class.java)
    }

    override fun doExecute(task: Task, request: ReplicateIndexRequest, listener: ActionListener<ReplicateIndexResponse>) {
        launch(threadPool.coroutineContext()) {
            listener.completeWith {
                log.info("Setting-up replication for ${request.leaderAlias}:${request.leaderIndex} -> ${request.followerIndex}")
                val user = SecurityContext.fromSecurityThreadContext(threadPool.threadContext)

                val followerReplContext = ReplicationContext(request.followerIndex,
                        user?.overrideFgacRole(request.useRoles?.get(ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE)))
                val leaderReplContext = ReplicationContext(request.leaderIndex,
                        user?.overrideFgacRole(request.useRoles?.get(ReplicateIndexRequest.LEADER_CLUSTER_ROLE)))

                // For autofollow request, setup checks are already made during addition of the pattern with
                // original user
                if(!request.isAutoFollowRequest) {
                    val setupChecksReq = SetupChecksRequest(followerReplContext, leaderReplContext, request.leaderAlias)
                    val setupChecksRes = client.suspendExecute(SetupChecksAction.INSTANCE, setupChecksReq)
                    if(!setupChecksRes.isAcknowledged) {
                        log.error("Setup checks failed while triggering replication for ${request.leaderAlias}:${request.leaderIndex} -> " +
                                "${request.followerIndex}")
                        throw org.opensearch.replication.ReplicationException("Setup checks failed while setting-up replication for ${request.followerIndex}")
                    }
                }

                // Any checks on the settings is followed by setup checks to ensure all relevant changes are
                // present across the plugins
                // validate index metadata on the leader cluster
                log.debug("Fetching leader cluster state for ${request.leaderIndex} index.")
                val leaderClusterState = getLeaderClusterState(request.leaderAlias, request.leaderIndex)
                ValidationUtil.validateLeaderIndexState(request.leaderAlias, request.leaderIndex, leaderClusterState)

                val leaderSettings = getLeaderIndexSettings(request.leaderAlias, request.leaderIndex)
                log.debug("Leader settings were fetched for ${request.leaderIndex} index.")

                if (leaderSettings.keySet().contains(ReplicationPlugin.REPLICATED_INDEX_SETTING.key) and
                        !leaderSettings.get(ReplicationPlugin.REPLICATED_INDEX_SETTING.key).isNullOrBlank()) {
                    throw IllegalArgumentException("Cannot Replicate a Replicated Index ${request.leaderIndex}")
                }

                // Soft deletes should be enabled for replication to work.
                if (!leaderSettings.getAsBoolean(IndexSettings.INDEX_SOFT_DELETES_SETTING.key, false)) {
                    throw IllegalArgumentException("Cannot Replicate an index where the setting ${IndexSettings.INDEX_SOFT_DELETES_SETTING.key} is disabled")
                }
                //Not starting replication if leader index is knn as knn plugin is not installed on follower.
                ValidationUtil.checkKNNEligibility(leaderSettings, clusterService, request.leaderIndex)

                ValidationUtil.validateIndexSettings(
                    environment,
                    request.followerIndex,
                    leaderSettings,
                    request.settings,
                    metadataCreateIndexService
                )

                // Setup checks are successful and trigger replication for the index
                // permissions evaluation to trigger replication is based on the current security context set
                val internalReq = ReplicateIndexClusterManagerNodeRequest(user, request)
                log.debug("Starting replication index action on current master node")
                client.suspendExecute(ReplicateIndexClusterManagerNodeAction.INSTANCE, internalReq)
                log.debug("Response of start replication action is returned")
                ReplicateIndexResponse(true)
            }
        }
    }


    private suspend fun getLeaderClusterState(leaderAlias: String, leaderIndex: String): ClusterState {
        val remoteClusterClient = client.getRemoteClusterClient(leaderAlias)
        val clusterStateRequest = remoteClusterClient.admin().cluster().prepareState()
                .clear()
                .setIndices(leaderIndex)
                .setRoutingTable(true)
                .setMetadata(true)
                .setIndicesOptions(IndicesOptions.strictSingleIndexNoExpandForbidClosed())
                .request()
        return remoteClusterClient.suspending(remoteClusterClient.admin().cluster()::state,
                injectSecurityContext = true, defaultContext = true)(clusterStateRequest).state
    }

    private suspend fun getLeaderIndexSettings(leaderAlias: String, leaderIndex: String): Settings {
        val remoteClient = client.getRemoteClusterClient(leaderAlias)
        val getSettingsRequest = GetSettingsRequest().includeDefaults(true).indices(leaderIndex)
        val settingsResponse = remoteClient.suspending(
            remoteClient.admin().indices()::getSettings,
            injectSecurityContext = true
        )(getSettingsRequest)

        val leaderSettings = settingsResponse.indexToSettings.get(leaderIndex)
            ?: throw IndexNotFoundException("${leaderAlias}:${leaderIndex}")
        val leaderDefaultSettings = settingsResponse.indexToDefaultSettings.get(leaderIndex)
            ?: throw IndexNotFoundException("${leaderAlias}:${leaderIndex}")

        // Since we want user configured as well as default settings, we combine both by putting default settings
        // and then the explicitly set ones to override the default settings.
        return Settings.builder().put(leaderDefaultSettings).put(leaderSettings).build()
    }
}
