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

package com.amazon.elasticsearch.replication.action.stats

import com.amazon.elasticsearch.replication.task.autofollow.AutoFollowTask
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.FailedNodeException
import org.elasticsearch.action.TaskOperationFailure
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.tasks.TransportTasksAction
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService

class TransportAutoFollowStatsAction @Inject constructor(transportService: TransportService,
                                                         clusterService: ClusterService,
                                                         actionFilters: ActionFilters
                                                            ) :
        TransportTasksAction<AutoFollowTask, AutoFollowStatsRequest, AutoFollowStatsResponses, AutoFollowStatsResponse>(AutoFollowStatsAction.NAME,
              clusterService, transportService,  actionFilters,
                ::AutoFollowStatsRequest,  ::AutoFollowStatsResponses, ::AutoFollowStatsResponse, ThreadPool.Names.MANAGEMENT), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportAutoFollowStatsAction::class.java)
    }


    override fun newResponse(request: AutoFollowStatsRequest, resp: MutableList<AutoFollowStatsResponse>, failure: MutableList<TaskOperationFailure>, failedNode: MutableList<FailedNodeException>): AutoFollowStatsResponses {
       return AutoFollowStatsResponses(resp,failedNode, failure )
    }

    override fun taskOperation(req: AutoFollowStatsRequest, task: AutoFollowTask, listener: ActionListener<AutoFollowStatsResponse>) {
        listener.onResponse(AutoFollowStatsResponse(task.status))
    }
}

