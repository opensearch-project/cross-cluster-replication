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

package com.amazon.elasticsearch.replication.util

import com.amazon.elasticsearch.replication.ReplicationException
import com.amazon.elasticsearch.replication.repository.RemoteClusterRepository
import com.amazon.elasticsearch.replication.metadata.store.ReplicationContext
import com.amazon.elasticsearch.replication.metadata.store.ReplicationMetadata
import com.amazon.opendistroforelasticsearch.commons.authuser.User
import kotlinx.coroutines.delay
import org.apache.logging.log4j.Logger
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.action.ActionType
import org.elasticsearch.action.index.IndexRequestBuilder
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.support.TransportActions
import org.elasticsearch.client.Client
import org.elasticsearch.common.util.concurrent.ThreadContext
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.index.store.Store
import org.elasticsearch.indices.recovery.RecoveryState
import org.elasticsearch.repositories.IndexId
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.snapshots.SnapshotId
import org.elasticsearch.transport.ConnectTransportException
import org.elasticsearch.transport.NodeDisconnectedException
import org.elasticsearch.transport.NodeNotConnectedException
import java.io.PrintWriter
import java.io.StringWriter

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
 * @param retryOn - javaClass name of Elasticsearch exceptions that should be retried along with default retryable exceptions
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

    repeat(numberOfRetries - 1) {
        try {
            return suspendExecute(replicationMetadata, action, req,
                    injectSecurityContext = injectSecurityContext, defaultContext = defaultContext)
        } catch (e: ElasticsearchException) {
            // Not retrying for IndexNotFoundException as it is not a transient failure
            // TODO Remove this check for IndexNotFoundException: https://github.com/opensearch-project/cross-cluster-replication/issues/78
            if (e !is IndexNotFoundException && (retryOn.contains(e.javaClass)
                            || TransportActions.isShardNotAvailableException(e)
                            // This waits for the dependencies to load and retry. Helps during boot-up
                            || e.status().status >= 500
                            || e.status() == RestStatus.TOO_MANY_REQUESTS)) {
                log.warn("Encountered a failure while executing in $req. Retrying in ${currentBackoff/1000} seconds" +
                        ".", e)
                delay(currentBackoff)
                currentBackoff = (currentBackoff * factor).toLong().coerceAtMost(maxTimeOut)
            } else {
                throw e
            }
        }
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
 * @param retryOn - javaClass name of Elasticsearch exceptions that should be retried along with default retryable exceptions
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
        } catch (e: ElasticsearchException) {
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
