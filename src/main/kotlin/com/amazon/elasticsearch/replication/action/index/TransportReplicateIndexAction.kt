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

package com.amazon.elasticsearch.replication.action.index

import com.amazon.elasticsearch.replication.ReplicationException
import com.amazon.elasticsearch.replication.ReplicationPlugin.Companion.REPLICATED_INDEX_SETTING
import com.amazon.elasticsearch.replication.action.setup.SetupChecksAction
import com.amazon.elasticsearch.replication.action.setup.SetupChecksRequest
import com.amazon.elasticsearch.replication.metadata.store.ReplicationContext
import com.amazon.elasticsearch.replication.util.SecurityContext
import com.amazon.elasticsearch.replication.util.ValidationUtil
import com.amazon.elasticsearch.replication.util.completeWith
import com.amazon.elasticsearch.replication.util.coroutineContext
import com.amazon.elasticsearch.replication.util.overrideFgacRole
import com.amazon.elasticsearch.replication.util.suspendExecute
import com.amazon.elasticsearch.replication.util.suspending
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.client.Client
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.env.Environment
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.indices.InvalidIndexNameException
import org.elasticsearch.tasks.Task
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService

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
        log.info("Setting-up replication for ${request.remoteCluster}:${request.remoteIndex} -> ${request.followerIndex}")
        val user = SecurityContext.fromSecurityThreadContext(threadPool.threadContext)
        launch(threadPool.coroutineContext()) {
            listener.completeWith {
                if(request.remoteIndex.startsWith(".")) {
                    throw InvalidIndexNameException(request.remoteIndex,"Cannot start replication for an index starting with '.'")
                }
                val remoteClient = client.getRemoteClusterClient(request.remoteCluster)
                val getSettingsRequest = GetSettingsRequest().includeDefaults(false).indices(request.remoteIndex)
                val settingsResponse = remoteClient.suspending(remoteClient.admin().indices()::getSettings, injectSecurityContext = true)(getSettingsRequest)
                val leaderSettings = settingsResponse.indexToSettings.get(request.remoteIndex) ?: throw IndexNotFoundException(request.remoteIndex)

                if (leaderSettings.keySet().contains(REPLICATED_INDEX_SETTING.key) and !leaderSettings.get(REPLICATED_INDEX_SETTING.key).isNullOrBlank()) {
                    throw IllegalArgumentException("Cannot Replicate a Replicated Index ${request.remoteIndex}")
                }
                ValidationUtil.validateAnalyzerSettings(environment, leaderSettings, request.settings)

                val followerReplContext = ReplicationContext(request.followerIndex,
                        user?.overrideFgacRole(request.assumeRoles?.get(ReplicateIndexRequest.FOLLOWER_FGAC_ROLE)))
                val leaderReplContext = ReplicationContext(request.remoteIndex,
                        user?.overrideFgacRole(request.assumeRoles?.get(ReplicateIndexRequest.LEADER_FGAC_ROLE)))

                // For autofollow request, setup checks are already made during addition of the pattern with
                // original user
                if(!request.isAutoFollowRequest) {
                    val setupChecksReq = SetupChecksRequest(followerReplContext, leaderReplContext, request.remoteCluster)
                    val setupChecksRes = client.suspendExecute(SetupChecksAction.INSTANCE, setupChecksReq)
                    if(!setupChecksRes.isAcknowledged) {
                        log.error("Setup checks failed while triggering replication for ${request.remoteCluster}:${request.remoteIndex} -> " +
                                "${request.followerIndex}")
                        throw ReplicationException("Setup checks failed while setting-up replication for ${request.followerIndex}")
                    }
                }

                // Setup checks are successful and trigger replication for the index
                // permissions evaluation to trigger replication is based on the current security context set
                val internalReq = ReplicateIndexMasterNodeRequest(user, request)
                client.suspendExecute(ReplicateIndexMasterNodeAction.INSTANCE, internalReq)
                ReplicateIndexResponse(true)
            }
        }
    }
}
