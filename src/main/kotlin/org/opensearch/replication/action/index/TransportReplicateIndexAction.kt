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

package org.opensearch.replication.action.index

import org.opensearch.replication.ReplicationException
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
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.client.Client
import org.opensearch.common.inject.Inject
import org.opensearch.env.Environment
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.IndexSettings
import org.opensearch.indices.InvalidIndexNameException
import org.opensearch.tasks.Task
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService

class TransportReplicateIndexAction @Inject constructor(transportService: TransportService,
                                                        val threadPool: ThreadPool,
                                                        actionFilters: ActionFilters,
                                                        private val client : Client,
                                                        private val environment: Environment) :
        HandledTransportAction<ReplicateIndexRequest, ReplicateIndexResponse>(ReplicateIndexAction.NAME,
                transportService, actionFilters, ::ReplicateIndexRequest),
    CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportReplicateIndexAction::class.java)
    }

    override fun doExecute(task: Task, request: ReplicateIndexRequest, listener: ActionListener<ReplicateIndexResponse>) {
        log.info("Setting-up replication for ${request.leaderAlias}:${request.leaderIndex} -> ${request.followerIndex}")
        val user = SecurityContext.fromSecurityThreadContext(threadPool.threadContext)
        launch(threadPool.coroutineContext()) {
            listener.completeWith {

                val followerReplContext = ReplicationContext(request.followerIndex,
                        user?.overrideFgacRole(request.assumeRoles?.get(ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE)))
                val leaderReplContext = ReplicationContext(request.leaderIndex,
                        user?.overrideFgacRole(request.assumeRoles?.get(ReplicateIndexRequest.LEADER_CLUSTER_ROLE)))

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

                val remoteClient = client.getRemoteClusterClient(request.leaderAlias)
                val getSettingsRequest = GetSettingsRequest().includeDefaults(false).indices(request.leaderIndex)
                val settingsResponse = remoteClient.suspending(remoteClient.admin().indices()::getSettings, injectSecurityContext = true)(getSettingsRequest)
                val leaderSettings = settingsResponse.indexToSettings.get(request.leaderIndex) ?: throw IndexNotFoundException(request.leaderIndex)

                if (leaderSettings.keySet().contains(ReplicationPlugin.REPLICATED_INDEX_SETTING.key) and
                        !leaderSettings.get(ReplicationPlugin.REPLICATED_INDEX_SETTING.key).isNullOrBlank()) {
                    throw IllegalArgumentException("Cannot Replicate a Replicated Index ${request.leaderIndex}")
                }
                if (!leaderSettings.getAsBoolean(IndexSettings.INDEX_SOFT_DELETES_SETTING.key, true)) {
                    throw IllegalArgumentException("Cannot Replicate an index where the setting ${IndexSettings.INDEX_SOFT_DELETES_SETTING.key} is disabled")
                }
                ValidationUtil.validateAnalyzerSettings(environment, leaderSettings, request.settings)

                // Setup checks are successful and trigger replication for the index
                // permissions evaluation to trigger replication is based on the current security context set
                val internalReq = ReplicateIndexMasterNodeRequest(user, request)
                client.suspendExecute(ReplicateIndexMasterNodeAction.INSTANCE, internalReq)
                ReplicateIndexResponse(true)
            }
        }
    }
}
