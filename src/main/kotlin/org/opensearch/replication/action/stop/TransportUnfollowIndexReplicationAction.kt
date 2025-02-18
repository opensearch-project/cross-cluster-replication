package org.opensearch.replication.action.stop

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.ActionRequest
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.common.inject.Inject
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.commons.replication.action.ReplicationActions.STOP_REPLICATION_ACTION_TYPE
import org.opensearch.commons.replication.action.StopIndexReplicationRequest
import org.opensearch.commons.replication.action.ReplicationActions.UNFOLLOW_REPLICATION_ACTION_NAME
import org.opensearch.commons.utils.recreateObject
import org.opensearch.core.action.ActionListener
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.stackTraceToString
import org.opensearch.replication.util.suspendExecute
import org.opensearch.tasks.Task
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService


class TransportUnfollowIndexReplicationAction @Inject constructor (
    val name: String,
    val transportService: TransportService,
    val clusterService: ClusterService,
    val threadPool: ThreadPool,
    val client: Client,
    val actionFilters: ActionFilters,
    val indexNameExpressionResolver: IndexNameExpressionResolver,
    val replicationMetadataManager: ReplicationMetadataManager,
): HandledTransportAction<ActionRequest, AcknowledgedResponse> (UNFOLLOW_REPLICATION_ACTION_NAME, transportService, actionFilters, ::StopIndexReplicationRequest),
    CoroutineScope by GlobalScope  {
    companion object {
        private val log = LogManager.getLogger(TransportUnfollowIndexReplicationAction::class.java)
    }

    @Throws(Exception::class)
    override fun doExecute(task: Task?, request: ActionRequest, listener: ActionListener<AcknowledgedResponse>?) {
        launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            val transformedRequest = request as? StopIndexReplicationRequest
                ?: request.let { recreateObject(it) { StopIndexReplicationRequest(it) } }
            try {

                var response = client.suspendExecute(STOP_REPLICATION_ACTION_TYPE, transformedRequest, true)
                log.info("Stop replication successful for index[${transformedRequest.indexName}], response: " + response.isAcknowledged)
                listener?.onResponse(AcknowledgedResponse(true))
            } catch (e: Exception) {
                log.error("Stop replication failed for index[${transformedRequest.indexName}] with error ${e.stackTraceToString()}")
                listener?.onFailure(e)
                throw e
            }
        }
    }
}