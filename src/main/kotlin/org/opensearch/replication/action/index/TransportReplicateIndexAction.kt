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

import org.opensearch.replication.util.SecurityContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
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
        log.trace("Starting replication for ${request.remoteCluster}:${request.remoteIndex} -> ${request.followerIndex}")

        // Captures the security context and triggers relevant operation on the master
        try {
            val user = SecurityContext.fromSecurityThreadContext(threadPool.threadContext)
            log.debug("Obtained security context - $user")
            val req = ReplicateIndexMasterNodeRequest(user, request)
            client.execute(ReplicateIndexMasterNodeAction.INSTANCE, req, object: ActionListener<AcknowledgedResponse>{
                override fun onFailure(e: Exception) {
                    listener.onFailure(e)
                }

                override fun onResponse(response: AcknowledgedResponse) {
                    listener.onResponse(ReplicateIndexResponse(response.isAcknowledged))
                }
            })
        } catch (e: Exception) {
            log.error("Failed to trigger replication for ${request.followerIndex} - $e")
            listener.onFailure(e)
        }

    }
}
