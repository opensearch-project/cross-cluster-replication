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

package com.amazon.elasticsearch.replication.action.changes

import com.amazon.elasticsearch.replication.util.completeWith
import com.amazon.elasticsearch.replication.util.coroutineContext
import com.amazon.elasticsearch.replication.util.waitForGlobalCheckpoint
import com.amazon.elasticsearch.replication.ReplicationPlugin.Companion.REPLICATION_EXECUTOR_NAME_LEADER
import com.amazon.elasticsearch.replication.seqno.RemoteClusterStats
import com.amazon.elasticsearch.replication.seqno.RemoteClusterTranslogService
import com.amazon.elasticsearch.replication.seqno.RemoteShardMetric
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchTimeoutException
import org.elasticsearch.ResourceNotFoundException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.routing.ShardsIterator
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.IndexSettings
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.index.translog.Translog
import org.elasticsearch.indices.IndicesService
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportActionProxy
import org.elasticsearch.transport.TransportService
import java.util.concurrent.TimeUnit
import kotlin.math.min

class TransportGetChangesAction @Inject constructor(threadPool: ThreadPool, clusterService: ClusterService,
                                                    transportService: TransportService, actionFilters: ActionFilters,
                                                    indexNameExpressionResolver: IndexNameExpressionResolver,
                                                    private val indicesService: IndicesService,
                                                    private val translogService: RemoteClusterTranslogService,
                                                    private val remoteStatsService: RemoteClusterStats) :
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
                var relativeStartNanos  = System.nanoTime()
                remoteStatsService.stats[shardId] = remoteStatsService.stats.getOrDefault(shardId, RemoteShardMetric())
                val indexMetric = remoteStatsService.stats[shardId]!!

                indexMetric.lastFetchTime.set(relativeStartNanos)

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
                        throw ElasticsearchTimeoutException("global checkpoint not synced. Retry after a few miliseconds...")
                    }
                }

                relativeStartNanos  = System.nanoTime()
                // At this point lastSyncedGlobalCheckpoint is at least fromSeqNo
                val toSeqNo = min(indexShard.lastSyncedGlobalCheckpoint, request.toSeqNo)

                var ops: List<Translog.Operation> = listOf()
                var fetchFromTranslog = isTranslogPruningByRetentionLeaseEnabled(shardId)
                if(fetchFromTranslog) {
                    try {
                        ops = translogService.getHistoryOfOperations(indexShard, request.fromSeqNo, toSeqNo)
                    } catch (e: ResourceNotFoundException) {
                        fetchFromTranslog = false
                    }
                }

                // Translog fetch is disabled or not found
                if(!fetchFromTranslog) {
                    log.debug("Fetching changes from lucene for ${request.shardId} - from:${request.fromSeqNo}, to:$toSeqNo")
                    relativeStartNanos  = System.nanoTime()
                    indexShard.newChangesSnapshot("odr", request.fromSeqNo, toSeqNo, true).use { snapshot ->
                        ops = ArrayList(snapshot.totalOperations())
                        var op = snapshot.next()
                        while (op != null) {
                            (ops as ArrayList<Translog.Operation>).add(op)
                            op = snapshot.next()
                        }
                    }
                }

                val tookInNanos = System.nanoTime() - relativeStartNanos
                val tookInMillis = TimeUnit.NANOSECONDS.toMillis(tookInNanos)
                if (fetchFromTranslog) {
                    indexMetric.latencyTlog.addAndGet(tookInMillis)
                    indexMetric.opsTlog.addAndGet(ops.size.toLong())
                } else {
                    indexMetric.latencyLucene.addAndGet(tookInMillis)
                    indexMetric.opsLucene.addAndGet(ops.size.toLong())
                }
                indexMetric.tlogSize.set(indexShard.translogStats().translogSizeInBytes)
                indexMetric.ops.addAndGet(ops.size.toLong())

                ops.stream().forEach{op -> indexMetric.bytesRead.addAndGet(op.estimateSize()) }

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