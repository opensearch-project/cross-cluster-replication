package org.opensearch.replication.action.stop

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import org.apache.logging.log4j.LogManager
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.ActionRequest
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.common.inject.Inject
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.commons.replication.action.StopIndexReplicationRequest
import org.opensearch.commons.replication.action.ReplicationActions.STOP_REPLICATION_BASE_ACTION_NAME
import org.opensearch.commons.utils.recreateObject
import org.opensearch.core.action.ActionListener
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.util.stackTraceToString
import org.opensearch.tasks.Task
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService


class TransportStopIndexReplicationBaseAction @Inject constructor (
    val name: String,
    val transportService: TransportService,
    val clusterService: ClusterService,
    val threadPool: ThreadPool,
    val client: Client,
    val actionFilters: ActionFilters,
    val indexNameExpressionResolver: IndexNameExpressionResolver,
    val replicationMetadataManager: ReplicationMetadataManager,
): HandledTransportAction<ActionRequest, AcknowledgedResponse> (STOP_REPLICATION_BASE_ACTION_NAME, transportService, actionFilters, ::StopIndexReplicationRequest),
    CoroutineScope by GlobalScope  {
    companion object {
        private val log = LogManager.getLogger(TransportStopIndexReplicationBaseAction::class.java)
    }
    val transportStopIndexReplicationAction = TransportStopIndexReplicationAction(
        transportService,
        clusterService,
        threadPool,
        actionFilters,
        indexNameExpressionResolver,
        client,
        replicationMetadataManager
    )

    @Throws(Exception::class)
    override fun doExecute(task: Task?, request: ActionRequest, listener: ActionListener<AcknowledgedResponse>?) {
        val transformedRequest = request as? StopIndexReplicationRequest
            ?: request.let { recreateObject(it) { StopIndexReplicationRequest(it) } }
        try {
            transportStopIndexReplicationAction.execute(transformedRequest, listener)
        } catch (e: Exception) {
            log.error("Stop replication failed for index[${transformedRequest.indexName}] with error ${e.stackTraceToString()}")
            listener?.onFailure(e)
            throw e
        }
    }
}