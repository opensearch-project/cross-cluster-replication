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

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchTimeoutException
import org.opensearch.core.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.single.shard.TransportSingleShardAction
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.routing.ShardsIterator
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.common.unit.TimeValue
import org.opensearch.core.index.shard.ShardId
import org.opensearch.index.shard.IndexShard
import org.opensearch.index.translog.Translog
import org.opensearch.indices.IndicesService
import org.opensearch.replication.ReplicationPlugin.Companion.REPLICATION_INDEX_TRANSLOG_PRUNING_ENABLED_SETTING
import org.opensearch.replication.ReplicationPlugin.Companion.REPLICATION_EXECUTOR_NAME_LEADER
import org.opensearch.replication.seqno.RemoteClusterStats
import org.opensearch.replication.seqno.RemoteClusterTranslogService
import org.opensearch.replication.seqno.RemoteShardMetric
import org.opensearch.replication.util.*
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportActionProxy
import org.opensearch.transport.TransportService
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
        log.debug("calling asyncShardOperation method")
        GlobalScope.launch(threadPool.coroutineContext(REPLICATION_EXECUTOR_NAME_LEADER)) {
            // TODO: Figure out if we need to acquire a primary permit here
            log.debug("$REPLICATION_EXECUTOR_NAME_LEADER coroutine has initiated")
            listener.completeWith {
                var relativeStartNanos  = System.nanoTime()
                remoteStatsService.stats[shardId] = remoteStatsService.stats.getOrDefault(shardId, RemoteShardMetric())
                val indexMetric = remoteStatsService.stats[shardId]!!

                indexMetric.lastFetchTime.set(relativeStartNanos)

                val indexShard = indicesService.indexServiceSafe(shardId.index).getShard(shardId.id)
                val isRemoteEnabledOrMigrating = ValidationUtil.isRemoteEnabledOrMigrating(clusterService)
                if (lastGlobalCheckpoint(indexShard, isRemoteEnabledOrMigrating) < request.fromSeqNo) {
                    // There are no new operations to sync. Do a long poll and wait for GlobalCheckpoint to advance. If
                    // the checkpoint doesn't advance by the timeout this throws an ESTimeoutException which the caller
                    // should catch and start a new poll.
                    log.trace("Waiting for global checkpoint to advance from ${request.fromSeqNo} Sequence Number")
                    val gcp = indexShard.waitForGlobalCheckpoint(request.fromSeqNo, WAIT_FOR_NEW_OPS_TIMEOUT)
                    log.trace("Waiting for global checkpoint to advance is finished for ${request.fromSeqNo} Sequence Number")
                    // At this point indexShard.lastKnownGlobalCheckpoint  has advanced but it may not yet have been synced
                    // to the translog, which means we can't return those changes. Return to the caller to retry.
                    // TODO: Figure out a better way to wait for the global checkpoint to be synced to the translog
                    if (lastGlobalCheckpoint(indexShard, isRemoteEnabledOrMigrating) < request.fromSeqNo) {
                        assert(gcp > lastGlobalCheckpoint(indexShard, isRemoteEnabledOrMigrating)) { "Checkpoint didn't advance at all $gcp ${lastGlobalCheckpoint(indexShard, isRemoteEnabledOrMigrating)}" }
                        throw OpenSearchTimeoutException("global checkpoint not synced. Retry after a few miliseconds...")
                    }
                }

                relativeStartNanos  = System.nanoTime()
                // At this point lastSyncedGlobalCheckpoint is at least fromSeqNo
                val toSeqNo = min(lastGlobalCheckpoint(indexShard, isRemoteEnabledOrMigrating), request.toSeqNo)

                var ops: List<Translog.Operation> = listOf()
                var fetchFromTranslog = isTranslogPruningByRetentionLeaseEnabled(shardId) && isRemoteEnabledOrMigrating == false
                if(fetchFromTranslog) {
                    try {
                        ops = translogService.getHistoryOfOperations(indexShard, request.fromSeqNo, toSeqNo)
                    } catch (e: Exception) {
                        log.debug("Fetching changes from translog for ${request.shardId} " +
                                "- from:${request.fromSeqNo}, to:$toSeqNo failed with exception - ${e.stackTraceToString()}")
                        fetchFromTranslog = false
                    }
                }

                // Translog fetch is disabled or not found
                if(!fetchFromTranslog) {
                    log.debug("Fetching changes from lucene for ${request.shardId} - from:${request.fromSeqNo}, to:$toSeqNo")
                    relativeStartNanos  = System.nanoTime()
                    indexShard.newChangesSnapshot("odr", request.fromSeqNo, toSeqNo, true, true).use { snapshot ->
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
                GetChangesResponse(ops, request.fromSeqNo, indexShard.maxSeqNoOfUpdatesOrDeletes, lastGlobalCheckpoint(indexShard, isRemoteEnabledOrMigrating))
            }
        }
    }

    private fun lastGlobalCheckpoint(indexShard: IndexShard, isRemoteEnabledOrMigrating: Boolean): Long {
        // We rely on lastSyncedGlobalCheckpoint as it has been durably written to disk. In case of remote store
        // enabled clusters, the semantics are slightly different, and we can't use lastSyncedGlobalCheckpoint. Falling back to
        // lastKnownGlobalCheckpoint in such cases.
        return if (isRemoteEnabledOrMigrating) {
            indexShard.lastKnownGlobalCheckpoint
        } else {
            indexShard.lastSyncedGlobalCheckpoint
        }
    }


    private fun isTranslogPruningByRetentionLeaseEnabled(shardId: ShardId): Boolean {
        val enabled = clusterService.state().metadata.indices.get(shardId.indexName)
                ?.settings?.getAsBoolean(REPLICATION_INDEX_TRANSLOG_PRUNING_ENABLED_SETTING.key, false)
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
        val shardIt = state.routingTable().shardRoutingTable(request.request().shardId)
        // Random active shards
        return if (ValidationUtil.isRemoteEnabledOrMigrating(clusterService)) shardIt.primaryShardIt()
        else shardIt.activeInitializingShardsRandomIt()
    }
}