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

package org.opensearch.replication.action.changes

import org.opensearch.replication.util.completeWith
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.waitForGlobalCheckpoint
import org.opensearch.replication.ReplicationPlugin.Companion.REPLICATION_EXECUTOR_NAME_LEADER
import org.opensearch.replication.seqno.RemoteClusterTranslogService
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchTimeoutException
import org.opensearch.ResourceNotFoundException
import org.opensearch.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.single.shard.TransportSingleShardAction
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.routing.ShardsIterator
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.unit.TimeValue
import org.opensearch.index.IndexSettings
import org.opensearch.index.shard.ShardId
import org.opensearch.index.translog.Translog
import org.opensearch.indices.IndicesService
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportActionProxy
import org.opensearch.transport.TransportService
import java.lang.IllegalStateException
import kotlin.math.min

class TransportGetChangesAction @Inject constructor(threadPool: ThreadPool, clusterService: ClusterService,
                                                    transportService: TransportService, actionFilters: ActionFilters,
                                                    indexNameExpressionResolver: IndexNameExpressionResolver,
                                                    private val indicesService: IndicesService,
                                                    private val translogService: RemoteClusterTranslogService) :
    TransportSingleShardAction<GetChangesRequest, GetChangesResponse>(
        GetChangesAction.NAME, threadPool, clusterService, transportService, actionFilters,
        indexNameExpressionResolver, ::GetChangesRequest, REPLICATION_EXECUTOR_NAME_LEADER) {

    init {
        TransportActionProxy.registerProxyAction(transportService, GetChangesAction.NAME, ::GetChangesResponse)
    }

    companion object {
        val WAIT_FOR_NEW_OPS_TIMEOUT = TimeValue.timeValueMinutes(1)!!
        private val log = LogManager.getLogger(TransportGetChangesAction::class.java)
    }

    override fun shardOperation(request: GetChangesRequest, shardId: ShardId): GetChangesResponse {
        throw UnsupportedOperationException("use asyncShardOperation")
    }

    @Suppress("BlockingMethodInNonBlockingContext")
    override fun asyncShardOperation(request: GetChangesRequest, shardId: ShardId, listener: ActionListener<GetChangesResponse>) {
        GlobalScope.launch(threadPool.coroutineContext(REPLICATION_EXECUTOR_NAME_LEADER)) {
            // TODO: Figure out if we need to acquire a primary permit here
            listener.completeWith {
                val indexShard = indicesService.indexServiceSafe(shardId.index).getShard(shardId.id)
                if (indexShard.lastSyncedGlobalCheckpoint < request.fromSeqNo) {
                    // There are no new operations to sync. Do a long poll and wait for GlobalCheckpoint to advance. If
                    // the checkpoint doesn't advance by the timeout this throws an ESTimeoutException which the caller
                    // should catch and start a new poll.
                    val gcp = indexShard.waitForGlobalCheckpoint(request.fromSeqNo, WAIT_FOR_NEW_OPS_TIMEOUT)

                    // At this point indexShard.lastKnownGlobalCheckpoint  has advanced but it may not yet have been synced
                    // to the translog, which means we can't return those changes. Return to the caller to retry.
                    // TODO: Figure out a better way to wait for the global checkpoint to be synced to the translog
                    if (indexShard.lastSyncedGlobalCheckpoint < request.fromSeqNo) {
                        assert(gcp > indexShard.lastSyncedGlobalCheckpoint) { "Checkpoint didn't advance at all" }
                        throw OpenSearchTimeoutException("global checkpoint not synced. Retry after a few miliseconds...")
                    }
                }

                // At this point lastSyncedGlobalCheckpoint is at least fromSeqNo
                val toSeqNo = min(indexShard.lastSyncedGlobalCheckpoint, request.toSeqNo)

                var ops: List<Translog.Operation> = listOf()
                var fetchFromTranslog = isTranslogPruningByRetentionLeaseEnabled(shardId)
                if(fetchFromTranslog) {
                    try {
                        ops = translogService.getHistoryOfOperations(indexShard, request.fromSeqNo, toSeqNo)
                    } catch (e: ResourceNotFoundException) {
                        fetchFromTranslog = false
                    } catch (e: IllegalStateException) {
                        fetchFromTranslog = false
                    }
                }

                // Translog fetch is disabled or not found
                if(!fetchFromTranslog) {
                    log.info("Fetching changes from lucene for ${request.shardId} - from:${request.fromSeqNo}, to:$toSeqNo")
                    indexShard.newChangesSnapshot("odr", request.fromSeqNo, toSeqNo, true).use { snapshot ->
                        ops = ArrayList(snapshot.totalOperations())
                        var op = snapshot.next()
                        while (op != null) {
                            (ops as ArrayList<Translog.Operation>).add(op)
                            op = snapshot.next()
                        }
                    }
                }
                GetChangesResponse(ops, request.fromSeqNo, indexShard.maxSeqNoOfUpdatesOrDeletes, indexShard.lastSyncedGlobalCheckpoint)
            }
        }
    }


    private fun isTranslogPruningByRetentionLeaseEnabled(shardId: ShardId): Boolean {
        val enabled = clusterService.state().metadata.indices.get(shardId.indexName)
                ?.settings?.getAsBoolean(IndexSettings.INDEX_TRANSLOG_RETENTION_LEASE_PRUNING_ENABLED_SETTING.key, false)
        if(enabled != null) {
            return enabled
        }
        return false
    }

    override fun resolveIndex(request: GetChangesRequest): Boolean {
        return true
    }

    override fun getResponseReader(): Writeable.Reader<GetChangesResponse> {
        return Writeable.Reader { inp: StreamInput -> GetChangesResponse(inp) }
    }

    override fun shards(state: ClusterState, request: InternalRequest): ShardsIterator {
        // Random active shards
        return state.routingTable().shardRoutingTable(request.request().shardId).activeInitializingShardsRandomIt()
    }
}