package com.amazon.elasticsearch.replication.action.setup

import com.amazon.elasticsearch.replication.metadata.store.ReplicationContext
import com.amazon.elasticsearch.replication.util.SecurityContext
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.util.concurrent.ThreadContext
import org.elasticsearch.tasks.Task
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService

class TransportSetupChecksAction @Inject constructor(transportService: TransportService,
                                                    val threadPool: ThreadPool,
                                                    actionFilters: ActionFilters,
                                                    private val client : Client,
                                                    private val clusterService: ClusterService) :
        HandledTransportAction<SetupChecksRequest, AcknowledgedResponse>(SetupChecksAction.NAME,
                transportService, actionFilters, ::SetupChecksRequest) {

    companion object {
        private val log = LogManager.getLogger(TransportSetupChecksAction::class.java)
    }

    override fun doExecute(task: Task, request: SetupChecksRequest, listener: ActionListener<AcknowledgedResponse>) {
        var remoteClusterClient: Client? = null
        val localClusterName = clusterService.clusterName.value()
        try {
            remoteClusterClient = client.getRemoteClusterClient(request.connectionName)
        } catch (e: Exception) {
            listener.onFailure(e)
        }

        // Validate permissions at both follower and leader cluster
        triggerPermissionsValidation(client, localClusterName, request.followerContext,
                object: ActionListener<ValidatePermissionsResponse> {
                        override fun onFailure(e: Exception) {
                            listener.onFailure(e)
                        }
                        override fun onResponse(response: ValidatePermissionsResponse) {
                            triggerPermissionsValidation(remoteClusterClient!!, request.connectionName,
                                    request.leaderContext,
                                    object : ActionListener<ValidatePermissionsResponse>{
                                        override fun onFailure(e: Exception) {
                                            listener.onFailure(e)
                                        }

                                        override fun onResponse(response: ValidatePermissionsResponse) {
                                            listener.onResponse(response)
                                        }
                                    })
                        }
                })

    }

    private fun triggerPermissionsValidation(client: Client,
                                             cluster: String,
                                             replContext: ReplicationContext,
                                             permissionListener: ActionListener<ValidatePermissionsResponse>) {

        var storedContext: ThreadContext.StoredContext? = null
        try {
            // Remove the assume roles transient from the previous call
            storedContext = client.threadPool().threadContext.newStoredContext(false,
                    listOf(SecurityContext.OPENDISTRO_SECURITY_ASSUME_ROLES))
            val assumeRole = replContext.user?.roles?.get(0)
            val inThreadContextRole = client.threadPool().threadContext.getTransient<String?>(SecurityContext.OPENDISTRO_SECURITY_ASSUME_ROLES)
            log.info("assume role is $inThreadContextRole for $cluster")
            client.threadPool().threadContext.putTransient(SecurityContext.OPENDISTRO_SECURITY_ASSUME_ROLES, assumeRole)
            val validateReq = ValidatePermissionsRequest(cluster, replContext.resource, assumeRole)
            client.execute(ValidatePermissionsAction.INSTANCE, validateReq, permissionListener)
        } finally {
            storedContext?.close()
        }
    }
}