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

package org.opensearch.replication.util

import org.opensearch.replication.repository.RemoteClusterRepository
import org.opensearch.replication.metadata.store.ReplicationContext
import org.opensearch.replication.metadata.store.ReplicationMetadata
import org.opensearch.commons.authuser.User
import kotlinx.coroutines.delay
import org.apache.logging.log4j.Logger
import org.opensearch.OpenSearchException
import org.opensearch.OpenSearchSecurityException
import org.opensearch.ResourceNotFoundException
import org.opensearch.core.action.ActionListener
import org.opensearch.action.ActionRequest
import org.opensearch.core.action.ActionResponse
import org.opensearch.action.ActionType
import org.opensearch.action.index.IndexRequestBuilder
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.support.TransportActions
import org.opensearch.transport.client.Client
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException
import org.opensearch.index.IndexNotFoundException
import org.opensearch.core.index.shard.ShardId
import org.opensearch.index.store.Store
import org.opensearch.indices.recovery.RecoveryState
import org.opensearch.replication.ReplicationException
import org.opensearch.replication.util.stackTraceToString
import org.opensearch.repositories.IndexId
import org.opensearch.core.rest.RestStatus
import org.opensearch.snapshots.SnapshotId
import org.opensearch.transport.ConnectTransportException
import org.opensearch.transport.NodeDisconnectedException
import org.opensearch.transport.NodeNotConnectedException
import java.io.PrintWriter
import java.io.StringWriter
import java.lang.Exception

/*
 * Extension function to use the store object
 */
fun Store.performOp(tryBlock: () -> Unit, finalBlock: () -> Unit = {}) {
    incRef()
    try {
        tryBlock()
    } finally {
        finalBlock()
        decRef()
    }
}

fun <T: ActionResponse>Client.execute(replMetadata: ReplicationMetadata, actionType: ActionType<T>,
                                      actionRequest: ActionRequest, timeout: Long): T {
    var storedContext: ThreadContext.StoredContext? = null
    try {
        storedContext = threadPool().threadContext.stashContext()
        SecurityContext.setBasedOnActions(replMetadata, actionType.name(), threadPool().threadContext)
        return execute(actionType, actionRequest).actionGet(timeout)
    } finally {
        storedContext?.restore()
    }
}

fun <T>Client.executeBlockUnderSecurityContext(replContext: ReplicationContext, block: () -> T): T {
    var storedContext: ThreadContext.StoredContext? = null
    try {
        storedContext = threadPool().threadContext.stashContext()
        SecurityContext.asRolesInjection(this.threadPool().threadContext, replContext.user?.toInjectedRoles())
        return block()
    } finally {
        storedContext?.restore()
    }
}

@Suppress("UNUSED_PARAMETER")
fun IndexRequestBuilder.execute(id: String, listener: ActionListener<IndexResponse>) {
    execute(listener)
}
/**
 * Retries a given block of code.
 * Only specified error are retried
 *
 * @param numberOfRetries - Number of retries
 * @param backoff - Retry interval
 * @param maxTimeOut - Time out for retries
 * @param factor - ExponentialBackoff factor
 * @param log - logger used to log intermediate failures
 * @param retryOn - javaClass name of OpenSearch exceptions that should be retried along with default retryable exceptions
 * @param block - the block of code to retry. This should be a suspend function.
 */
suspend fun <Req: ActionRequest, Resp: ActionResponse> Client.suspendExecuteWithRetries(
        replicationMetadata: ReplicationMetadata?,
        action: ActionType<Resp>,
        req: Req,
        numberOfRetries: Int = 5,
        backoff: Long = 10000,        // 10 seconds
        maxTimeOut: Long = 600000,    // 10 minutes
        factor: Double = 2.0,
        log: Logger,
        retryOn: ArrayList<Class<*>> = ArrayList(),
        injectSecurityContext: Boolean = false,
        defaultContext: Boolean = false): Resp {
    var currentBackoff = backoff
    retryOn.addAll(defaultRetryableExceptions())
    var retryException: Exception
    repeat(numberOfRetries - 1) { index ->
        try {
            log.debug("Sending get changes request after ${currentBackoff / 1000} seconds.")
            return suspendExecute(replicationMetadata, action, req,
                    injectSecurityContext = injectSecurityContext, defaultContext = defaultContext)
        } catch (e: OpenSearchException) {
            // Not retrying for IndexNotFoundException as it is not a transient failure
            // TODO Remove this check for IndexNotFoundException: https://github.com/opensearch-project/cross-cluster-replication/issues/78
            if (e !is IndexNotFoundException && (retryOn.contains(e.javaClass)
                            || TransportActions.isShardNotAvailableException(e)
                            // This waits for the dependencies to load and retry. Helps during boot-up
                            || e.status().status >= 500
                            || e.status() == RestStatus.TOO_MANY_REQUESTS)) {
                retryException = e;
            } else {
                throw e
            }
        } catch (e: OpenSearchRejectedExecutionException) {
            if(index < numberOfRetries-2) {
                retryException = e;
            }
            else {
                throw ReplicationException(e, RestStatus.TOO_MANY_REQUESTS)
            }
        }
        log.warn(
            "Encountered a failure(can be ignored) while getting changes:  OpenSearchTimeoutException. Retrying in ${currentBackoff / 1000} seconds" +
                    "."
        )
        delay(currentBackoff)
        currentBackoff = (currentBackoff * factor).toLong().coerceAtMost(maxTimeOut)

    }
    return suspendExecute(replicationMetadata, action, req,
            injectSecurityContext = injectSecurityContext, defaultContext = defaultContext) // last attempt
}
/**
 * Restore shard from leader cluster with retries.
 * Only specified error are retried
 *
 * @param numberOfRetries - Number of retries
 * @param backoff - Retry interval
 * @param maxTimeOut - Time out for retries
 * @param factor - ExponentialBackoff factor
 * @param log - logger used to log intermediate failures
 * @param retryOn - javaClass name of OpenSearch exceptions that should be retried along with default retryable exceptions
 */
suspend fun RemoteClusterRepository.restoreShardWithRetries(
        store: Store, snapshotId: SnapshotId, indexId: IndexId, snapshotShardId: ShardId,
        recoveryState: RecoveryState, listener: ActionListener<Void>,
        function: suspend (Store, SnapshotId, IndexId, ShardId, RecoveryState, ActionListener<Void>) -> Unit,
        numberOfRetries: Int = 5,
        backoff: Long = 10000,        // 10 seconds
        maxTimeOut: Long = 600000,    // 10 minutes
        factor: Double = 2.0,
        log: Logger,
        retryOn: ArrayList<Class<*>> = ArrayList()
) {
    var currentBackoff = backoff
    var retryCount = 1 // we retry 4 times after first exception. In total we are running callable function 5 times.
    retryOn.addAll(defaultRetryableExceptions())
    repeat(numberOfRetries) {
        try {
            return function(store, snapshotId, indexId, snapshotShardId, recoveryState, listener)
        } catch (e: OpenSearchException) {
            if (retryOn.contains(e.javaClass) && retryCount < numberOfRetries) {
                log.warn("Encountered a failure during restore shard. Retrying in ${currentBackoff / 1000} seconds.", e)
                delay(currentBackoff)
                currentBackoff = (currentBackoff * factor).toLong().coerceAtMost(maxTimeOut)
                retryCount++
            } else {
                log.error("Restore of shard from remote cluster repository failed permanently after all retries due to ${e.stackTraceToString()}")
                store.decRef()
                listener.onFailure(e)
                return
            }
        }
    }
}

private fun defaultRetryableExceptions(): ArrayList<Class<*>> {
    val retryableExceptions = ArrayList<Class<*>>()
    retryableExceptions.add(NodeDisconnectedException::class.java)
    retryableExceptions.add(NodeNotConnectedException::class.java)
    retryableExceptions.add(ConnectTransportException::class.java)
    return retryableExceptions
}

fun User.overrideFgacRole(fgacRole: String?): User? {
    var roles = emptyList<String>()
    if(fgacRole != null) {
        roles = listOf(fgacRole)
    }
    return User(this.name, this.backendRoles, roles,
            this.customAttNames, this.requestedTenant)
}

fun User.toInjectedUser(): String? {
    return "${SecurityContext.REPLICATION_PLUGIN_USER}|${backendRoles.joinToString(separator=",")}"
}

fun User.toInjectedRoles(): String? {
    return "${SecurityContext.REPLICATION_PLUGIN_USER}|${roles.joinToString(separator=",")}"
}

fun Throwable.stackTraceToString(): String {
    val sw = StringWriter()
    val pw = PrintWriter(sw)
    printStackTrace(pw)
    pw.flush()
    return sw.toString()
}
