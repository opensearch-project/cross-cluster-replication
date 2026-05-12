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

package org.opensearch.replication.action.autofollow

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
import org.opensearch.core.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.transport.client.Client
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
                    val followerClusterRole = request.useRoles?.get(ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE)
                    val leaderClusterRole = request.useRoles?.get(ReplicateIndexRequest.LEADER_CLUSTER_ROLE)
                    val setupChecksRequest = SetupChecksRequest(ReplicationContext(request.pattern!!, user?.overrideFgacRole(followerClusterRole)),
                            ReplicationContext(request.pattern!!, user?.overrideFgacRole(leaderClusterRole)),
                            request.connection)
                    val setupChecksRes = client.suspendExecute(SetupChecksAction.INSTANCE, setupChecksRequest)
                    if(!setupChecksRes.isAcknowledged) {
                        throw org.opensearch.replication.ReplicationException("Setup checks failed while setting-up auto follow pattern")
                    }
                }
                val clusterManagerNodeReq = AutoFollowClusterManagerNodeRequest(user, request)
                client.suspendExecute(AutoFollowClusterManagerNodeAction.INSTANCE, clusterManagerNodeReq)
            }
        }
    }
}
