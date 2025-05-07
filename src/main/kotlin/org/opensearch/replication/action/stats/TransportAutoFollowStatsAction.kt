/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.replication.action.stats

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import org.apache.logging.log4j.LogManager
import org.opensearch.action.FailedNodeException
import org.opensearch.action.TaskOperationFailure
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.tasks.TransportTasksAction
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.core.action.ActionListener
import org.opensearch.replication.task.autofollow.AutoFollowTask
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService

class TransportAutoFollowStatsAction @Inject constructor(
    transportService: TransportService,
    clusterService: ClusterService,
    actionFilters: ActionFilters,
) :
    TransportTasksAction<AutoFollowTask, AutoFollowStatsRequest, AutoFollowStatsResponses, AutoFollowStatsResponse>(
        AutoFollowStatsAction.NAME,
        clusterService, transportService, actionFilters,
        ::AutoFollowStatsRequest, ::AutoFollowStatsResponses, ::AutoFollowStatsResponse, ThreadPool.Names.MANAGEMENT,
    ),
    CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportAutoFollowStatsAction::class.java)
    }

    override fun newResponse(request: AutoFollowStatsRequest, resp: MutableList<AutoFollowStatsResponse>, failure: MutableList<TaskOperationFailure>, failedNode: MutableList<FailedNodeException>): AutoFollowStatsResponses {
        return AutoFollowStatsResponses(resp, failedNode, failure)
    }

    override fun taskOperation(req: AutoFollowStatsRequest, task: AutoFollowTask, listener: ActionListener<AutoFollowStatsResponse>) {
        listener.onResponse(AutoFollowStatsResponse(task.status))
    }
}
