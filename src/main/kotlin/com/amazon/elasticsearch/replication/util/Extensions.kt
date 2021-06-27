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

import com.amazon.elasticsearch.replication.repository.RemoteClusterRepository
import kotlinx.coroutines.delay
import org.apache.logging.log4j.Logger
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.action.ActionType
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.index.store.Store
import org.elasticsearch.indices.recovery.RecoveryState
import org.elasticsearch.repositories.IndexId
import org.elasticsearch.snapshots.SnapshotId
import org.elasticsearch.transport.ConnectTransportException
import org.elasticsearch.transport.NodeDisconnectedException
import org.elasticsearch.transport.NodeNotConnectedException

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

fun <T> Client.executeUnderSecurityContext(clusterService: ClusterService,
                                           remoteClusterName: String,
                                           followerIndexName: String,
                                           block: () -> T) {
    val userString = SecurityContext.fromClusterState(clusterService.state(),
            remoteClusterName, followerIndexName)
    this.threadPool().threadContext.newStoredContext(true).use {
        SecurityContext.toThreadContext(this.threadPool().threadContext, userString)
        block()
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
 * @param retryOn - javaClass name of Elasticsearch exceptions that should be retried along with default retryable exceptions
 * @param block - the block of code to retry. This should be a suspend function.
 */
suspend fun <Req : ActionRequest, Resp : ActionResponse> Client.suspendExecuteWithRetries(
        action: ActionType<Resp>,
        req: Req,
        numberOfRetries: Int = 5,
        backoff: Long = 10000,        // 10 seconds
        maxTimeOut: Long = 600000,    // 10 minutes
        factor: Double = 2.0,
        log: Logger,
        retryOn: ArrayList<Class<*>> = ArrayList()): Resp {
    var currentBackoff = backoff
    retryOn.addAll(defaultRetryableExceptions())
    repeat(numberOfRetries - 1) {
        try {
            return suspendExecute(action, req)
        } catch (e: ElasticsearchException) {
            if (retryOn.contains(e.javaClass)) {
                log.warn("Encountered a failure while executing $req. Retrying in ${currentBackoff / 1000} seconds.", e)
                delay(currentBackoff)
                currentBackoff = (currentBackoff * factor).toLong().coerceAtMost(maxTimeOut)
            } else {
                throw e
            }
        }
    }
    return suspendExecute(action, req) // last attempt
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
    var retryCount = 1
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
                log.error("Restore of shard from remote cluster repository failed permanently after all retries due to $e")
                store.decRef()
                listener.onFailure(e)
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


