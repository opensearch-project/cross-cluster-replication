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

package org.opensearch.replication.action.setup

import org.opensearch.replication.util.completeWith
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

class TransportValidatePermissionsAction @Inject constructor(transportService: TransportService,
                                                             val threadPool: ThreadPool,
                                                             actionFilters: ActionFilters,
                                                             private val client : Client) :
        HandledTransportAction<ValidatePermissionsRequest, AcknowledgedResponse>(ValidatePermissionsAction.NAME,
                transportService, actionFilters, ::ValidatePermissionsRequest) {


    companion object {
        private val log = LogManager.getLogger(TransportValidatePermissionsAction::class.java)
    }

    override fun doExecute(task: Task, request: ValidatePermissionsRequest, listener: ActionListener<AcknowledgedResponse>) {
        log.info("Replication setup - Permissions validation successful for Index - ${request.index} and role ${request.clusterRole}")
        listener.completeWith { AcknowledgedResponse(true) }
    }

}
