package com.amazon.elasticsearch.replication.action.setup

import com.amazon.elasticsearch.replication.ReplicationException
import com.amazon.elasticsearch.replication.metadata.store.ReplicationContext
import com.amazon.elasticsearch.replication.util.SecurityContext
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchSecurityException
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.StepListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.CheckedConsumer
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.util.concurrent.ThreadContext
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.tasks.Task
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService
import java.util.function.Consumer
import java.util.function.Predicate

class TransportSetupChecksAction @Inject constructor(transportService: TransportService,
                                                    val threadPool: ThreadPool,
                                                    actionFilters: ActionFilters,
                                                    private val client : Client,
                                                    private val clusterService: ClusterService) :
        HandledTransportAction<SetupChecksRequest, AcknowledgedResponse>(SetupChecksAction.NAME,
                transportService, actionFilters, ::SetupChecksRequest) {

    companion object {
        private val log = LogManager.getLogger(TransportSetupChecksAction::class.java)
        fun unwrapSecurityExceptionIfPresent(e: Exception): Exception {
            val ex = ExceptionsHelper.unwrapCausesAndSuppressed<Exception>(e) { cause ->
                cause is ElasticsearchSecurityException
            }
            if(!ex.isPresent) {
                return e
            }
            val securityException = ex.get()
            return ElasticsearchSecurityException(securityException.message, RestStatus.FORBIDDEN, securityException.cause)
        }
    }

    override fun doExecute(task: Task, request: SetupChecksRequest, listener: ActionListener<AcknowledgedResponse>) {
        var remoteClusterClient: Client? = null
        val localClusterName = clusterService.clusterName.value()
        try {
            remoteClusterClient = client.getRemoteClusterClient(request.connectionName)
        } catch (e: Exception) {
            // Logging it as info as this check is to see if remote cluster is added or not
            log.info("Failed to connect to remote cluster $request.connectionName with error $e")
            listener.onFailure(e)
            return
        }

        // If user obj is present, security plugin is enabled. Roles are mandatory
        if(request.followerContext.user != null && request.followerContext.user!!.roles.isEmpty()) {
            log.info("User roles are empty for follower_resource:${request.followerContext.resource}")
            listener.onFailure(ElasticsearchSecurityException("Follower roles are mandatory for replication", RestStatus.FORBIDDEN))
            return
        }

        if(request.leaderContext.user != null && request.leaderContext.user!!.roles.isEmpty()) {
            log.info("User roles are empty for leader_resource:${request.leaderContext.resource}")
            listener.onFailure(ElasticsearchSecurityException("Leader roles are mandatory for replication", RestStatus.FORBIDDEN))
            return
        }

        val userPermissionsValidationAtLocal = StepListener<AcknowledgedResponse>()
        val userPermissionsValidationAtRemote = StepListener<AcknowledgedResponse>()
        val rolePermissionsValidationAtLocal = StepListener<AcknowledgedResponse>()
        val rolePermissionsValidationAtRemote = StepListener<AcknowledgedResponse>()

        rolePermissionsValidationAtRemote.whenComplete(
                { r ->
                    log.info("Permissions validation successful for role [connection:${request.connectionName}, " +
                            "resource:${request.leaderContext.resource}]")
                    listener.onResponse(r)
                },
                { e ->
                    log.error("Permissions validation failed for role [connection:${request.connectionName}, " +
                            "resource:${request.leaderContext.resource}] with $e")
                    listener.onFailure(unwrapSecurityExceptionIfPresent(e))
                }
        )

        rolePermissionsValidationAtLocal.whenComplete(
                {
                    log.info("Permissions validation successful for User [connection:${request.connectionName}, " +
                            "resource:${request.leaderContext.resource}]")
                    triggerPermissionsValidation(remoteClusterClient!!, request.connectionName, request.leaderContext, true, rolePermissionsValidationAtRemote)
                },
                { e ->
                    log.error("Permissions validation failed for role [local:$localClusterName, " +
                            "resource:${request.followerContext.resource}] with $e")
                    listener.onFailure(unwrapSecurityExceptionIfPresent(e))
                }
        )

        userPermissionsValidationAtRemote.whenComplete(
                {
                    log.info("Permissions validation successful for User [connection:${request.connectionName}, " +
                            "resource:${request.leaderContext.resource}]")
                    triggerPermissionsValidation(client, localClusterName, request.followerContext, true, rolePermissionsValidationAtLocal)
                },
                { e ->
                    log.error("Permissions validation failed for User [connection:${request.connectionName}, " +
                            "resource:${request.leaderContext.resource}] with $e")
                    listener.onFailure(unwrapSecurityExceptionIfPresent(e))
                }
        )

        userPermissionsValidationAtLocal.whenComplete(
                {
                    log.info("Permissions validation successful for User [local:$localClusterName, " +
                            "resource:${request.followerContext.resource}]")
                    triggerPermissionsValidation(remoteClusterClient!!, request.connectionName, request.leaderContext, false, userPermissionsValidationAtRemote)
                },
                { e ->
                    log.error("Permissions validation failed for User [local:$localClusterName, " +
                            "resource:${request.followerContext.resource}] with $e")
                    listener.onFailure(unwrapSecurityExceptionIfPresent(e))
                }
        )

        triggerPermissionsValidation(client, localClusterName, request.followerContext, false, userPermissionsValidationAtLocal)

    }

    private fun triggerPermissionsValidation(client: Client,
                                             cluster: String,
                                             replContext: ReplicationContext,
                                             shouldAssumeRole: Boolean,
                                             permissionListener: ActionListener<AcknowledgedResponse>) {

        var storedContext: ThreadContext.StoredContext? = null
        try {
            // Remove the assume roles transient from the previous call
            storedContext = client.threadPool().threadContext.newStoredContext(false,
                    listOf(SecurityContext.OPENDISTRO_SECURITY_ASSUME_ROLES))
            val assumeRole = replContext.user?.roles?.get(0)
            val inThreadContextRole = client.threadPool().threadContext.getTransient<String?>(SecurityContext.OPENDISTRO_SECURITY_ASSUME_ROLES)
            log.debug("assume role is $inThreadContextRole for $cluster")
            if(shouldAssumeRole) {
                client.threadPool().threadContext.putTransient(SecurityContext.OPENDISTRO_SECURITY_ASSUME_ROLES, assumeRole)
            }
            val validateReq = ValidatePermissionsRequest(cluster, replContext.resource, assumeRole)
            client.execute(ValidatePermissionsAction.INSTANCE, validateReq, permissionListener)
        } finally {
            storedContext?.close()
        }
    }
}