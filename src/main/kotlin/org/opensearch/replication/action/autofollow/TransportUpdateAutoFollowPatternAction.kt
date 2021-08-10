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

package org.opensearch.replication.action.autofollow

import org.opensearch.replication.ReplicationException
import org.opensearch.replication.action.index.ReplicateIndexRequest
import org.opensearch.replication.action.setup.SetupChecksAction
import org.opensearch.replication.action.setup.SetupChecksRequest
import org.opensearch.replication.metadata.store.ReplicationContext
import org.opensearch.replication.util.SecurityContext
import org.opensearch.replication.util.completeWith
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.overrideFgacRole
import org.opensearch.replication.util.suspendExecute
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.common.inject.Inject
import org.opensearch.tasks.Task
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService

class TransportUpdateAutoFollowPatternAction @Inject constructor(transportService: TransportService,
                                                                 val threadPool: ThreadPool,
                                                                 actionFilters: ActionFilters,
                                                                 private val client: Client) :
        HandledTransportAction<UpdateAutoFollowPatternRequest, AcknowledgedResponse>(UpdateAutoFollowPatternAction.NAME,
                transportService, actionFilters, ::UpdateAutoFollowPatternRequest), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportUpdateAutoFollowPatternAction::class.java)
    }

    override fun doExecute(task: Task, request: UpdateAutoFollowPatternRequest, listener: ActionListener<AcknowledgedResponse>) {
        log.info("Setting-up autofollow for ${request.connection}:${request.patternName} -> " +
                "${request.pattern}")
        val user = SecurityContext.fromSecurityThreadContext(threadPool.threadContext)
        launch(threadPool.coroutineContext()) {
            listener.completeWith {
                if (request.action == UpdateAutoFollowPatternRequest.Action.ADD) {
                    // Pattern is same for leader and follower
                    val followerClusterRole = request.assumeRoles?.get(ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE)
                    val leaderClusterRole = request.assumeRoles?.get(ReplicateIndexRequest.LEADER_CLUSTER_ROLE)
                    val setupChecksRequest = SetupChecksRequest(ReplicationContext(request.pattern!!, user?.overrideFgacRole(followerClusterRole)),
                            ReplicationContext(request.pattern!!, user?.overrideFgacRole(leaderClusterRole)),
                            request.connection)
                    val setupChecksRes = client.suspendExecute(SetupChecksAction.INSTANCE, setupChecksRequest)
                    if(!setupChecksRes.isAcknowledged) {
                        throw org.opensearch.replication.ReplicationException("Setup checks failed while setting-up auto follow pattern")
                    }
                }
                val masterNodeReq = AutoFollowMasterNodeRequest(user, request)
                client.suspendExecute(AutoFollowMasterNodeAction.INSTANCE, masterNodeReq)
            }
        }
    }
}
