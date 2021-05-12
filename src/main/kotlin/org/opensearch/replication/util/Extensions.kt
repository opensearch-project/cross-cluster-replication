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

package org.opensearch.replication.util

import kotlinx.coroutines.delay
import org.apache.logging.log4j.Logger
import org.opensearch.OpenSearchException
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionResponse
import org.opensearch.action.ActionType
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.index.store.Store
import org.opensearch.transport.ConnectTransportException
import org.opensearch.transport.NodeDisconnectedException

/*
 * Extension function to use the store object
 */
fun Store.performOp(tryBlock: () -> Unit, finalBlock: () -> Unit = {}) {
    incRef()
    try {
        tryBlock()
    }
    finally {
        finalBlock()
        decRef()
    }
}

fun <T>Client.executeUnderSecurityContext(clusterService: ClusterService,
                                          remoteClusterName: String,
                                          followerIndexName: String,
                                          block: () -> T) {
    val userString = SecurityContext.fromClusterState(clusterService.state(),
            remoteClusterName, followerIndexName)
    var storedContext: ThreadContext.StoredContext? = null
    try {
        SecurityContext.toThreadContext(this.threadPool().threadContext, userString)
        block()
    }
    finally {
        storedContext?.restore()
    }
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
        action: ActionType<Resp>,
        req: Req,
        numberOfRetries: Int = 5,
        backoff: Long = 10000,        // 10 seconds
        maxTimeOut: Long = 600000,    // 10 minutes
        factor: Double = 2.0,
        log: Logger,
        retryOn: ArrayList<Class<*>> = ArrayList()) : Resp {
    var currentBackoff = backoff
    retryOn.addAll(defaultRetryableExceptions())
    repeat(numberOfRetries - 1) {
        try {
            return suspendExecute(action, req)
        } catch (e: OpenSearchException) {
            if (retryOn.contains(e.javaClass)) {
                log.warn("Encountered a failure. Retrying in ${currentBackoff/1000} seconds.", e)
                delay(currentBackoff)
                currentBackoff = (currentBackoff * factor).toLong().coerceAtMost(maxTimeOut)
            } else {
                throw e
            }
        }
    }
    return suspendExecute(action, req) // last attempt
}

private fun defaultRetryableExceptions(): ArrayList<Class<*>> {
    val retryableExceptions = ArrayList<Class<*>>()
    retryableExceptions.add(NodeDisconnectedException::class.java)
    retryableExceptions.add(ConnectTransportException::class.java)
    return retryableExceptions
}


