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

import com.amazon.elasticsearch.replication.action.setup.SetupChecksAction
import com.amazon.elasticsearch.replication.action.setup.SetupChecksRequest
import com.amazon.elasticsearch.replication.action.setup.TransportSetupChecksAction
import com.amazon.elasticsearch.replication.metadata.store.ReplicationContext
import com.amazon.elasticsearch.replication.util.SecurityContext
import com.amazon.elasticsearch.replication.util.overrideFgacRole
import com.amazon.opendistroforelasticsearch.commons.authuser.User
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.util.concurrent.ThreadContext
import org.elasticsearch.tasks.Task
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService

class TransportReplicateIndexAction @Inject constructor(transportService: TransportService,
                                                        val threadPool: ThreadPool,
                                                        actionFilters: ActionFilters,
                                                        private val client : Client) :
        HandledTransportAction<ReplicateIndexRequest, ReplicateIndexResponse>(ReplicateIndexAction.NAME,
                transportService, actionFilters, ::ReplicateIndexRequest),
    CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportReplicateIndexAction::class.java)
    }

    override fun doExecute(task: Task, request: ReplicateIndexRequest, listener: ActionListener<ReplicateIndexResponse>) {
        log.info("Setting-up replication for ${request.remoteCluster}:${request.remoteIndex} -> ${request.followerIndex}")
        val user = SecurityContext.fromSecurityThreadContext(threadPool.threadContext)
        try {
            val followerReplContext = ReplicationContext(request.followerIndex,
                    user?.overrideFgacRole(request.assumeRoles?.get(ReplicateIndexRequest.FOLLOWER_FGAC_ROLE)))
            val leaderReplContext = ReplicationContext(request.remoteIndex,
                    user?.overrideFgacRole(request.assumeRoles?.get(ReplicateIndexRequest.LEADER_FGAC_ROLE)))

            val setupChecksReq = SetupChecksRequest(followerReplContext, leaderReplContext, request.remoteCluster)
            client.execute(SetupChecksAction.INSTANCE, setupChecksReq, object: ActionListener<AcknowledgedResponse>{
                override fun onFailure(e: Exception) {
                    listener.onFailure(e)
                }

                override fun onResponse(response: AcknowledgedResponse) {
                    triggerReplication(user, request, listener)
                }
            })
        } catch (e: Exception) {
            log.error("Failed to trigger replication for ${request.followerIndex} - $e")
            listener.onFailure(e)
            return
        }
    }


    private fun triggerReplication(user: User?, request: ReplicateIndexRequest,
                                   listener: ActionListener<ReplicateIndexResponse>) {
        val internalReq = ReplicateIndexMasterNodeRequest(user, request)
        val inThreadContextRole = client.threadPool().threadContext.getTransient<String?>(SecurityContext.OPENDISTRO_SECURITY_ASSUME_ROLES)
        log.info("assume role for trigger Replication is $inThreadContextRole")
        var storedContext: ThreadContext.StoredContext? = null
        try {
            storedContext = client.threadPool().threadContext.newStoredContext(false,
                    listOf(SecurityContext.OPENDISTRO_SECURITY_ASSUME_ROLES))
            client.execute(ReplicateIndexMasterNodeAction.INSTANCE, internalReq, object: ActionListener<AcknowledgedResponse> {
                override fun onFailure(e: java.lang.Exception) {
                    listener.onFailure(e)
                }

                override fun onResponse(response: AcknowledgedResponse) {
                    listener.onResponse(ReplicateIndexResponse(response.isAcknowledged))
                }
            })
        } finally {
            storedContext?.close()
        }
    }
}
