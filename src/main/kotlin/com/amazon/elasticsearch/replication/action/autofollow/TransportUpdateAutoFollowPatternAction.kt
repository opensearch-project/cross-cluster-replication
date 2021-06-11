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

package com.amazon.elasticsearch.replication.action.autofollow

import com.amazon.elasticsearch.replication.ReplicationException
import com.amazon.elasticsearch.replication.action.index.ReplicateIndexAction
import com.amazon.elasticsearch.replication.action.index.ReplicateIndexRequest
import com.amazon.elasticsearch.replication.action.index.ReplicateIndexResponse
import com.amazon.elasticsearch.replication.action.index.TransportReplicateIndexAction
import com.amazon.elasticsearch.replication.action.setup.SetupChecksAction
import com.amazon.elasticsearch.replication.action.setup.SetupChecksRequest
import com.amazon.elasticsearch.replication.action.setup.ValidatePermissionsRequest
import com.amazon.elasticsearch.replication.metadata.ReplicationMetadataManager
import com.amazon.elasticsearch.replication.metadata.ReplicationOverallState
import com.amazon.elasticsearch.replication.metadata.store.ReplicationContext
import com.amazon.elasticsearch.replication.task.autofollow.AutoFollowExecutor
import com.amazon.elasticsearch.replication.task.autofollow.AutoFollowParams
import com.amazon.elasticsearch.replication.util.SecurityContext
import com.amazon.elasticsearch.replication.util.completeWith
import com.amazon.elasticsearch.replication.util.coroutineContext
import com.amazon.elasticsearch.replication.util.overrideFgacRole
import com.amazon.elasticsearch.replication.util.persistentTasksService
import com.amazon.elasticsearch.replication.util.removeTask
import com.amazon.elasticsearch.replication.util.startTask
import com.amazon.elasticsearch.replication.util.suspendExecute
import com.amazon.opendistroforelasticsearch.commons.authuser.User
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.ResourceAlreadyExistsException
import org.elasticsearch.ResourceNotFoundException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.support.master.TransportMasterNodeAction
import org.elasticsearch.client.Client
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.block.ClusterBlockLevel
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.tasks.Task
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService

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
                    val followerFgacRole = request.assumeRoles?.get(ReplicateIndexRequest.FOLLOWER_FGAC_ROLE)
                    val leaderFgacRole = request.assumeRoles?.get(ReplicateIndexRequest.LEADER_FGAC_ROLE)
                    val setupChecksRequest = SetupChecksRequest(ReplicationContext(request.pattern!!, user?.overrideFgacRole(followerFgacRole)),
                            ReplicationContext(request.pattern!!, user?.overrideFgacRole(leaderFgacRole)),
                            request.connection)
                    val setupChecksRes = client.suspendExecute(SetupChecksAction.INSTANCE, setupChecksRequest)
                    if(!setupChecksRes.isAcknowledged) {
                        throw ReplicationException("Setup checks failed while setting-up auto follow pattern")
                    }
                }
                val masterNodeReq = AutoFollowMasterNodeRequest(user, request)
                client.suspendExecute(AutoFollowMasterNodeAction.INSTANCE, masterNodeReq)
            }
        }
    }
}
