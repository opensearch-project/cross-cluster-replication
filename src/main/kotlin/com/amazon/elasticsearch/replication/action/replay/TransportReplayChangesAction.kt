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

package com.amazon.elasticsearch.replication.action.replay

import com.amazon.elasticsearch.replication.ReplicationException
import com.amazon.elasticsearch.replication.metadata.checkIfIndexBlockedWithLevel
import com.amazon.elasticsearch.replication.util.SecurityContext
import com.amazon.elasticsearch.replication.util.completeWith
import com.amazon.elasticsearch.replication.util.coroutineContext
import com.amazon.elasticsearch.replication.util.suspending
import com.amazon.elasticsearch.replication.util.waitForNextChange
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest
import org.elasticsearch.action.bulk.TransportShardBulkAction
import org.elasticsearch.action.resync.TransportResyncReplicationAction
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.replication.TransportWriteAction
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.ClusterStateObserver
import org.elasticsearch.cluster.action.index.MappingUpdatedAction
import org.elasticsearch.cluster.action.shard.ShardStateAction
import org.elasticsearch.cluster.block.ClusterBlockLevel
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.IndexingPressure
import org.elasticsearch.index.engine.Engine
import org.elasticsearch.index.shard.IndexShard
import org.elasticsearch.index.translog.Translog
import org.elasticsearch.indices.IndicesService
import org.elasticsearch.indices.SystemIndices
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService
import java.util.function.Function

/**
 * Similar to [TransportResyncReplicationAction] except it also writes the changes to the primary before replicating
 * to the replicas.  The source of changes is, of course, the remote cluster.
 */
class TransportReplayChangesAction @Inject constructor(settings: Settings, transportService: TransportService,
                                                       clusterService: ClusterService, indicesService: IndicesService,
                                                       threadPool: ThreadPool, shardStateAction: ShardStateAction,
                                                       actionFilters: ActionFilters,
                                                       indexingPressure: IndexingPressure,
                                                       systemIndices: SystemIndices,
                                                       private val client: Client,
                                                       // Unused for now because of a bug in creating the PutMappingRequest
                                                       private val mappingUpdatedAction: MappingUpdatedAction) :
    TransportWriteAction<ReplayChangesRequest, ReplayChangesRequest, ReplayChangesResponse>(
        settings, ACTION_NAME, transportService, clusterService, indicesService, threadPool, shardStateAction,
        actionFilters, Writeable.Reader { inp -> ReplayChangesRequest(inp) }, Writeable.Reader { inp -> ReplayChangesRequest(inp) },
            EXECUTOR_NAME_FUNCTION, false, indexingPressure, systemIndices) {

    companion object {
        const val ACTION_NAME = "indices:data/write/replication"
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
            it.withPrimaryTerm(primaryShard.operationPrimaryTerm)
        }.forEach { op ->
            if(primaryShard.maxSeqNoOfUpdatesOrDeletes < request.maxSeqNoOfUpdatesOrDeletes) {
                primaryShard.advanceMaxSeqNoOfUpdatesOrDeletes(request.maxSeqNoOfUpdatesOrDeletes)
            }
            var result = primaryShard.applyTranslogOperation(op, Engine.Operation.Origin.PRIMARY)
            if (result.resultType == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
                waitForMappingUpdate {
                    // fetch mappings from the remote cluster when applying on PRIMARY...
                    syncRemoteMapping(request.remoteCluster, request.remoteIndex, request.shardId()!!.indexName,
                        op.docType())
                }
                result = primaryShard.applyTranslogOperation(op, Engine.Operation.Origin.PRIMARY)
            }

            location = syncOperationResultOrThrow(result, location)
        }
        val response = ReplayChangesResponse() // TODO: Figure out what to add to response
        return WritePrimaryResult(request, response, location, null, primaryShard, log)
    }

    /**
     * This requires duplicating the code above due to mapping updates being asynchronous.
     */
    suspend fun performOnSecondary(request: ReplayChangesRequest, replicaShard: IndexShard)
        : WriteReplicaResult<ReplayChangesRequest> {

        checkIfIndexBlockedWithLevel(clusterService, request.index(), ClusterBlockLevel.WRITE)
        var location: Translog.Location? = null
        request.changes.asSequence().map {
            it.withPrimaryTerm(replicaShard.operationPrimaryTerm)
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

    private fun Translog.Operation.docType(): String {
        return when (this) {
            is Translog.Index -> type()
            is Translog.Delete -> type()
            else -> TODO("Operation ${opType()} not expected to have a document type")
        }
    }

    /**
     * Fetches the index mapping from the remote cluster, applies it to the local cluster's master and then waits
     * for the mapping to become available on the current shard. Should only be called on the primary shard .
     */
    private suspend fun syncRemoteMapping(remoteCluster: String, remoteIndex: String,
                                          followerIndex: String, type: String) {
        log.debug("Syncing mappings from ${remoteCluster}:${remoteIndex}/${type} -> $followerIndex...")
        val remoteClient = client.getRemoteClusterClient(remoteCluster)
        val options = IndicesOptions.strictSingleIndexNoExpandForbidClosed()
        val getMappingsRequest = GetMappingsRequest().indices(remoteIndex).indicesOptions(options)
        val user = threadPool.threadContext.getTransient<Object?>(SecurityContext.OPENDISTRO_SECURITY_USER)
        log.info("User obj is $user")
        val injectedHeader = threadPool.threadContext.getTransient<String?>(SecurityContext.OPENDISTRO_SECURITY_ASSUME_ROLES)
        log.info("Injected user obj is $injectedHeader")
        val getMappingsResponse = remoteClient.suspending(remoteClient.admin().indices()::getMappings)(getMappingsRequest)
        val mappingSource = getMappingsResponse.mappings().get(remoteIndex).get(type).source().string()

        // This should use MappingUpdateAction but that uses PutMappingRequest internally and
        // PutMappingRequest#setConcreteIndex has a bug where it throws an NPE.This is fixed upstream in
        // https://github.com/elastic/elasticsearch/pull/58419 and we should update to that when it is released.
        val putMappingRequest = PutMappingRequest().indices(followerIndex).indicesOptions(options)
            .type(type).source(mappingSource, XContentType.JSON)
            //TODO: call .masterNodeTimeout() with the setting indices.mapping.dynamic_timeout
        val putMappingResponse = client.suspending(client.admin().indices()::putMapping)(putMappingRequest)
        if (!putMappingResponse.isAcknowledged) {
            throw ReplicationException("failed to update mappings to match mapping in source clusters")
        } else {
            log.debug("Mappings synced for $followerIndex")
        }
    }

    /**
     * Waits for an index mapping update to become available on the current shard. If a [mappingUpdater] is provided
     * it will be called to fetch and update the mapping. The updater is normally run only on the primary shard to fetch
     * mappings from the remote index.  On replica shards an updater is not required as the primary should have already
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
                Translog.Index(sourceOp.type(), sourceOp.id(), sourceOp.seqNo(), operationPrimaryTerm,
                    sourceOp.version(), BytesReference.toBytes(sourceOp.source()),
                    sourceOp.routing(), sourceOp.autoGeneratedIdTimestamp)
            }
            Translog.Operation.Type.DELETE -> {
                val sourceOp = this as Translog.Delete
                Translog.Delete(sourceOp.type(), sourceOp.id(), sourceOp.uid(), sourceOp.seqNo(), operationPrimaryTerm,
                    sourceOp.version())
            }
            Translog.Operation.Type.NO_OP -> {
                val sourceOp = this as Translog.NoOp
                Translog.NoOp(sourceOp.seqNo(), operationPrimaryTerm, sourceOp.reason())
            }
        }
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

