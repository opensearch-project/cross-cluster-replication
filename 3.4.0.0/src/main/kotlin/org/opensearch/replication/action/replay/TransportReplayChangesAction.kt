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

package org.opensearch.replication.action.replay

import org.opensearch.replication.MappingNotAvailableException
import org.opensearch.replication.metadata.UpdateMetadataAction
import org.opensearch.replication.metadata.UpdateMetadataRequest
import org.opensearch.replication.metadata.checkIfIndexBlockedWithLevel
import org.opensearch.replication.util.completeWith
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.suspendExecute
import org.opensearch.replication.util.suspending
import org.opensearch.replication.util.waitForNextChange
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.core.action.ActionListener
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.resync.TransportResyncReplicationAction
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.replication.TransportWriteAction
import org.opensearch.transport.client.Client
import org.opensearch.cluster.ClusterStateObserver
import org.opensearch.cluster.action.index.MappingUpdatedAction
import org.opensearch.cluster.action.shard.ShardStateAction
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.service.ClusterService
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.common.inject.Inject
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.IndexingPressureService
import org.opensearch.index.engine.Engine
import org.opensearch.index.engine.Engine.NoOp
import org.opensearch.index.mapper.MapperParsingException
import org.opensearch.index.shard.IndexShard
import org.opensearch.index.translog.Translog
import org.opensearch.indices.IndicesService
import org.opensearch.indices.SystemIndices
import org.opensearch.telemetry.tracing.noop.NoopTracer
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import java.util.function.Function

/**
 * Similar to [TransportResyncReplicationAction] except it also writes the changes to the primary before replicating
 * to the replicas.  The source of changes is, of course, the leader cluster.
 */
class TransportReplayChangesAction @Inject constructor(settings: Settings, transportService: TransportService,
                                                       clusterService: ClusterService, indicesService: IndicesService,
                                                       threadPool: ThreadPool, shardStateAction: ShardStateAction,
                                                       actionFilters: ActionFilters,
                                                       indexingPressureService: IndexingPressureService,
                                                       systemIndices: SystemIndices,
                                                       private val client: Client,
                                                       // Unused for now because of a bug in creating the PutMappingRequest
                                                       private val mappingUpdatedAction: MappingUpdatedAction) :
    TransportWriteAction<ReplayChangesRequest, ReplayChangesRequest, ReplayChangesResponse>(
        settings, ReplayChangesAction.NAME, transportService, clusterService, indicesService, threadPool, shardStateAction,
        actionFilters, Writeable.Reader { inp -> ReplayChangesRequest(inp) }, Writeable.Reader { inp -> ReplayChangesRequest(inp) },
            EXECUTOR_NAME_FUNCTION, false, indexingPressureService, systemIndices, NoopTracer.INSTANCE) {

    companion object {
        private val log = LogManager.getLogger(TransportReplayChangesAction::class.java)!!
        private val EXECUTOR_NAME_FUNCTION = Function { shard: IndexShard ->
            if (shard.indexSettings().indexMetadata.isSystem) {
                ThreadPool.Names.SYSTEM_WRITE
            } else {
                ThreadPool.Names.WRITE
            }
        }
    }

    private val job = SupervisorJob()
    private val scope = CoroutineScope(threadPool.executor(ThreadPool.Names.WRITE).asCoroutineDispatcher() + job)

    override fun newResponseInstance(inp: StreamInput): ReplayChangesResponse = ReplayChangesResponse(inp)

    override fun dispatchedShardOperationOnPrimary(request: ReplayChangesRequest, primaryShard: IndexShard,
                                         listener: ActionListener<PrimaryResult<ReplayChangesRequest, ReplayChangesResponse>>) {

        scope.launch(threadPool.coroutineContext()) {
            listener.completeWith {
                performOnPrimary(request, primaryShard)
            }
        }
    }

    override fun dispatchedShardOperationOnReplica(request: ReplayChangesRequest, replica: IndexShard,
                                                   listener: ActionListener<ReplicaResult>) {
        scope.launch(threadPool.coroutineContext()) {
            listener.completeWith {
                performOnSecondary(request, replica)
            }
        }
    }

    suspend fun performOnPrimary(request: ReplayChangesRequest, primaryShard: IndexShard)
        : WritePrimaryResult<ReplayChangesRequest, ReplayChangesResponse> {

        checkIfIndexBlockedWithLevel(clusterService, request.index(), ClusterBlockLevel.WRITE)
        var location: Translog.Location? = null
        request.changes.asSequence().map {
            it.withPrimaryTerm(primaryShard.operationPrimaryTerm).unSetAutoGenTimeStamp()
        }.forEach { op ->
            if(primaryShard.maxSeqNoOfUpdatesOrDeletes < request.maxSeqNoOfUpdatesOrDeletes) {
                primaryShard.advanceMaxSeqNoOfUpdatesOrDeletes(request.maxSeqNoOfUpdatesOrDeletes)
            }

            var result = primaryShard.applyTranslogOperation(op, Engine.Operation.Origin.PRIMARY)
            if (shouldSyncMappingAndRetry(result)) {
                waitForMappingUpdate {
                    // fetch mappings from the leader cluster when applying on PRIMARY...
                    syncRemoteMapping(request.leaderAlias, request.leaderIndex, request.shardId()!!.indexName)
                }
                result = primaryShard.applyTranslogOperation(op, Engine.Operation.Origin.PRIMARY)
            }

            location = syncOperationResultOrThrow(result, location)
        }
        val response = ReplayChangesResponse() // TODO: Figure out what to add to response
        return WritePrimaryResult(request, response, location, null, primaryShard, log)
    }
    fun shouldSyncMappingAndRetry(result: Engine.Result): Boolean {
        /*
            1. Incase the doc index requires a mapping update, we get the result as MAPPING_UPDATE_REQUIRED.
            2. If the dynamic mapping is set to strict, IndexShard will simply reject the applyTranslogOperation operation
            as expected. This can happen if user has already updated the mapping on leader but its not present on the follower yet.
            So in both case, we sync the mapping from leader index and retry the applyTranslogOperation.
         */
        return result.resultType == Engine.Result.Type.MAPPING_UPDATE_REQUIRED || result.failure is MapperParsingException
    }

    /**
     * This requires duplicating the code above due to mapping updates being asynchronous.
     */
    suspend fun performOnSecondary(request: ReplayChangesRequest, replicaShard: IndexShard)
        : WriteReplicaResult<ReplayChangesRequest> {

        checkIfIndexBlockedWithLevel(clusterService, request.index(), ClusterBlockLevel.WRITE)
        var location: Translog.Location? = null
        request.changes.asSequence().map {
            it.withPrimaryTerm(replicaShard.operationPrimaryTerm).unSetAutoGenTimeStamp()
        }.forEach { op ->
            var result = replicaShard.applyTranslogOperation(op, Engine.Operation.Origin.REPLICA)
            if (result.resultType == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
                waitForMappingUpdate()
                result = replicaShard.applyTranslogOperation(op, Engine.Operation.Origin.REPLICA)
            }
            location = syncOperationResultOrThrow(result, location)
        }
        return WriteReplicaResult(request, location, null, replicaShard, log)
    }

    /**
     * Fetches the index mapping from the leader cluster, applies it to the local cluster's clusterManager and then waits
     * for the mapping to become available on the current shard. Should only be called on the primary shard .
     */
    private suspend fun syncRemoteMapping(leaderAlias: String, leaderIndex: String,
                                          followerIndex: String) {
        log.debug("Syncing mappings from ${leaderAlias}:${leaderIndex} -> $followerIndex...")
        val remoteClient = client.getRemoteClusterClient(leaderAlias)
        val options = IndicesOptions.strictSingleIndexNoExpandForbidClosed()
        val getMappingsRequest = GetMappingsRequest().indices(leaderIndex).indicesOptions(options)
        val getMappingsResponse = remoteClient.suspending(remoteClient.admin().indices()::getMappings, injectSecurityContext = true)(getMappingsRequest)
        val mappingSource = getMappingsResponse?.mappings()?.get(leaderIndex)?.source()?.string()
        if (null == mappingSource) {
            log.error("Mapping response: $getMappingsResponse")
            throw MappingNotAvailableException("Mapping for the index $leaderIndex is not available")
        }


        // This should use MappingUpdateAction but that uses PutMappingRequest internally and
        // PutMappingRequest#setConcreteIndex has a bug where it throws an NPE.This is fixed upstream in
        // https://github.com/elastic/elasticsearch/pull/58419 and we should update to that when it is released.
        val putMappingRequest = PutMappingRequest().indices(followerIndex).indicesOptions(options)
            .source(mappingSource, XContentType.JSON)
            //TODO: call .masterNodeTimeout() with the setting indices.mapping.dynamic_timeout
        val updateMappingRequest = UpdateMetadataRequest(followerIndex, UpdateMetadataRequest.Type.MAPPING, putMappingRequest)
        client.suspendExecute(UpdateMetadataAction.INSTANCE, updateMappingRequest, injectSecurityContext = true)
        log.debug("Mappings synced for $followerIndex")
    }

    /**
     * Waits for an index mapping update to become available on the current shard. If a [mappingUpdater] is provided
     * it will be called to fetch and update the mapping. The updater is normally run only on the primary shard to fetch
     * mappings from the leader index.  On replica shards an updater is not required as the primary should have already
     * updated the mapping - we just have to wait for it to reach this node.
     */
    private suspend fun waitForMappingUpdate(mappingUpdater: suspend () -> Unit = {}) {
        log.debug("Waiting for mapping update...")
        val clusterStateObserver = ClusterStateObserver(clusterService, log, threadPool.threadContext)
        mappingUpdater()
        clusterStateObserver.waitForNextChange("mapping update on replica")
        log.debug("Mapping updated.")
    }

    private fun Translog.Operation.withPrimaryTerm(operationPrimaryTerm: Long): Translog.Operation {
        @Suppress("DEPRECATION")
        return when (opType()!!) {
            Translog.Operation.Type.CREATE, Translog.Operation.Type.INDEX -> {
                val sourceOp = this as Translog.Index
                Translog.Index(sourceOp.id(), sourceOp.seqNo(), operationPrimaryTerm,
                    sourceOp.version(), BytesReference.toBytes(sourceOp.source()),
                    sourceOp.routing(), sourceOp.autoGeneratedIdTimestamp)
            }
            Translog.Operation.Type.DELETE -> {
                val sourceOp = this as Translog.Delete
                Translog.Delete(sourceOp.id(), sourceOp.seqNo(), operationPrimaryTerm,
                    sourceOp.version())
            }
            Translog.Operation.Type.NO_OP -> {
                val sourceOp = this as Translog.NoOp
                Translog.NoOp(sourceOp.seqNo(), operationPrimaryTerm, sourceOp.reason())
            }
        }
    }

    @Suppress("DEPRECATION")
    private fun Translog.Operation.unSetAutoGenTimeStamp(): Translog.Operation {
        // Unset auto gen timestamp as we use external Id from the leader index
        if (opType()!! == Translog.Operation.Type.CREATE || opType()!! == Translog.Operation.Type.INDEX ) {
            val sourceOp = this as Translog.Index
            return Translog.Index(sourceOp.id(), sourceOp.seqNo(), sourceOp.primaryTerm(),
                    sourceOp.version(), BytesReference.toBytes(sourceOp.source()),
                    sourceOp.routing(), IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP)
        }
        return this
    }

    override fun globalBlockLevel(): ClusterBlockLevel? {
        return ClusterBlockLevel.WRITE
    }

    override fun indexBlockLevel(): ClusterBlockLevel? {
        /* Ideally, we want to block if there is already a WRITE block added to cluster.
        However, we dont want to be blocked from our own replication write block.

        Since this method doesn't have access to actual block, we can't block on WRITE level
        from here without getting blocked from our own cluster-block. To mitigate this we would
        add code in our ReplayChanges action to check for other WRITE blocks(eg disk space block, etc)
        before going ahead with shard update.
         */
        return null
    }
}

