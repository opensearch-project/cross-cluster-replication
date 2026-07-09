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

package org.opensearch.replication.metadata

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.message.ParameterizedMessage
import org.opensearch.core.action.ActionListener
import org.opensearch.action.IndicesRequest
import org.opensearch.action.admin.indices.alias.IndicesAliasesClusterStateUpdateRequest
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions
import org.opensearch.action.admin.indices.close.CloseIndexClusterStateUpdateRequest
import org.opensearch.action.admin.indices.close.CloseIndexRequest
import org.opensearch.action.admin.indices.close.CloseIndexResponse
import org.opensearch.action.admin.indices.mapping.put.PutMappingClusterStateUpdateRequest
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest
import org.opensearch.action.admin.indices.open.OpenIndexClusterStateUpdateRequest
import org.opensearch.action.admin.indices.open.OpenIndexRequest
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsClusterStateUpdateRequest
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.ack.ClusterStateUpdateResponse
import org.opensearch.cluster.ack.OpenIndexClusterStateUpdateResponse
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.metadata.*
import org.opensearch.cluster.metadata.AliasAction.RemoveIndex
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.index.Index
import org.opensearch.index.IndexNotFoundException
import org.opensearch.replication.util.stackTraceToString
import org.opensearch.rest.action.admin.indices.AliasesNotFoundException
import org.opensearch.tasks.Task
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import java.util.*

/*
 This action allows the replication plugin to update the index metadata(mapping, setting & aliases) on the follower index
 when there is a metadata write block(added by the plugin).
 */
class TransportUpdateMetadataAction @Inject constructor(
    transportService: TransportService, actionFilters: ActionFilters, threadPool: ThreadPool,
    clusterService: ClusterService, indexNameExpressionResolver: IndexNameExpressionResolver,
    val metadataMappingService: MetadataMappingService,
    val updateSettingsService: MetadataUpdateSettingsService,
    val indexAliasService: MetadataIndexAliasesService,
    val indexStateService: MetadataIndexStateService
) : TransportClusterManagerNodeAction<UpdateMetadataRequest, AcknowledgedResponse>(UpdateMetadataAction.NAME,
    transportService, clusterService, threadPool, actionFilters, ::UpdateMetadataRequest, indexNameExpressionResolver) {

    companion object {
        private val log = LogManager.getLogger(TransportUpdateMetadataAction::class.java)
        private val indicesOptions = IndicesOptions.fromOptions(false, false, true, true)
    }

    override fun executor(): String = ThreadPool.Names.SAME
    override fun read(inp: StreamInput?) = AcknowledgedResponse(inp)
    override fun checkBlock(request: UpdateMetadataRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    override fun clusterManagerOperation(
        task: Task,
        request: UpdateMetadataRequest,
        state: ClusterState,
        listener: ActionListener<AcknowledgedResponse>
    ) {
        val concreteIndices = resolveIndices(state, request, indexNameExpressionResolver)
        when (request.type) {
            UpdateMetadataRequest.Type.SETTING -> {
                performSettingUpdate(concreteIndices, request, listener, updateSettingsService)
            }
            UpdateMetadataRequest.Type.MAPPING -> {
                performMappingUpdate(concreteIndices, request, listener, metadataMappingService)
            }
            UpdateMetadataRequest.Type.ALIAS -> {
                performAliasUpdate(concreteIndices, request, listener, indexAliasService, state)
            }
            UpdateMetadataRequest.Type.OPEN -> {
                performOpenIndex(concreteIndices, request, listener)
            }
            UpdateMetadataRequest.Type.CLOSE -> {
                performCloseIndex(task, concreteIndices, request, listener)
            }
        }
    }


    private fun performOpenIndex(concreteIndices: Array<Index>, request: UpdateMetadataRequest,
                                 listener: ActionListener<AcknowledgedResponse>) {
        val openIndexRequest = request.request as OpenIndexRequest
        val updateRequest = OpenIndexClusterStateUpdateRequest()
                .ackTimeout(openIndexRequest.timeout()).clusterManagerNodeTimeout(openIndexRequest.clusterManagerNodeTimeout())
                .indices(concreteIndices).waitForActiveShards(openIndexRequest.waitForActiveShards())

        indexStateService.openIndex(updateRequest, object : ActionListener<OpenIndexClusterStateUpdateResponse> {
            override fun onResponse(response: OpenIndexClusterStateUpdateResponse) {
                listener.onResponse(AcknowledgedResponse(response.isAcknowledged))
            }

            override fun onFailure(t: java.lang.Exception) {
                log.error({ ParameterizedMessage("failed to open indices [{}]", concreteIndices as Any) }, t)
                listener.onFailure(t)
            }
        })
    }

    private fun performCloseIndex(task :Task , concreteIndices: Array<Index>, request: UpdateMetadataRequest,
                                 listener: ActionListener<AcknowledgedResponse>) {
        val openIndexRequest = request.request as CloseIndexRequest
        val closeRequest = CloseIndexClusterStateUpdateRequest(task.id)
                .ackTimeout(openIndexRequest.timeout())
                .clusterManagerNodeTimeout(openIndexRequest.clusterManagerNodeTimeout())
                .waitForActiveShards(openIndexRequest.waitForActiveShards())
                .indices(concreteIndices)

        indexStateService.closeIndices(closeRequest, object : ActionListener<CloseIndexResponse> {
            override fun onResponse(response: CloseIndexResponse) {
                listener.onResponse(AcknowledgedResponse(response.isAcknowledged))
            }

            override fun onFailure(t: java.lang.Exception) {
                log.error({ ParameterizedMessage("failed to close indices [{}]", concreteIndices as Any) }, t)
                listener.onFailure(t)
            }
        })

    }

    private fun performAliasUpdate(concreteIndices: Array<Index>, request: UpdateMetadataRequest,
                                   listener: ActionListener<AcknowledgedResponse>,
                                   indexAliasService: MetadataIndexAliasesService, clusterState: ClusterState) {
        val indicesAliasesRequest = request.request as IndicesAliasesRequest
        val actions: List<AliasActions> = indicesAliasesRequest.aliasActions
        var finalActions: ArrayList<AliasAction> = ArrayList()

        // Resolve all the AliasActions into AliasAction instances and gather all the aliases
        val aliases: HashSet<String> = HashSet()
        for (action in actions) {
            for (concreteIndex in concreteIndices) {
                val indexAbstraction: IndexAbstraction =
                    clusterState.metadata().getIndicesLookup().get(concreteIndex.name)
                        ?: error("invalid cluster metadata. index [" + concreteIndex.name + "] was not found")
                require(indexAbstraction.parentDataStream == null) {
                    ("The provided expressions [" + java.lang.String.join(",", *action.indices())
                            + "] match a backing index belonging to data stream [" + indexAbstraction.parentDataStream?.getName()
                            + "]. Data streams and their backing indices don't support aliases.")
                }
            }

            Collections.addAll(aliases, *action.originalAliases)
            for (index in concreteIndices) {
                when (action.actionType()) {
                    AliasActions.Type.ADD -> for (alias in concreteAliases(action,clusterState.metadata(), index.name)) {
                        finalActions.add(AliasAction.Add(index.name, alias, action.filter(), action.indexRouting(),
                                action.searchRouting(), action.writeIndex(), action.isHidden))
                    }
                    AliasActions.Type.REMOVE -> for (alias in concreteAliases(action, clusterState.metadata(), index.name)) {
                        finalActions.add(AliasAction.Remove(index.name, alias, action.mustExist()))
                    }
                    AliasActions.Type.REMOVE_INDEX -> finalActions.add(RemoveIndex(index.name))
                    else -> throw IllegalArgumentException("Unsupported action [" + action.actionType() + "]")
                }
            }
        }

        if (finalActions.isEmpty() && !actions.isEmpty()) {
            throw AliasesNotFoundException(*aliases.toTypedArray())
        }

        val updateRequest =
            IndicesAliasesClusterStateUpdateRequest(Collections.unmodifiableList(finalActions))
                .ackTimeout(request.timeout()).clusterManagerNodeTimeout(request.clusterManagerNodeTimeout())

        indexAliasService.indicesAliases(
            updateRequest,
            object : ActionListener<ClusterStateUpdateResponse> {
                override fun onResponse(response: ClusterStateUpdateResponse) {
                    listener.onResponse(AcknowledgedResponse(response.isAcknowledged))
                }

                override fun onFailure(t: Exception) {
                    log.error("failed to perform aliases on index ${request.indexName}", t)
                    listener.onFailure(t)
                }
            })
    }

    private fun performSettingUpdate(concreteIndices: Array<Index>, request: UpdateMetadataRequest,
                                             listener: ActionListener<AcknowledgedResponse>,
                                             updateSettingsService: MetadataUpdateSettingsService) {
        val updateSettingsRequest = request.request as UpdateSettingsRequest
        val clusterStateUpdateRequest = UpdateSettingsClusterStateUpdateRequest()
            .indices(concreteIndices)
            .settings(updateSettingsRequest.settings())
            .setPreserveExisting(updateSettingsRequest.isPreserveExisting)
            .ackTimeout(request.timeout())
            .clusterManagerNodeTimeout(request.clusterManagerNodeTimeout())


        updateSettingsService.updateSettings(clusterStateUpdateRequest,
            object : ActionListener<ClusterStateUpdateResponse> {
                override fun onResponse(response: ClusterStateUpdateResponse) {
                    listener.onResponse(AcknowledgedResponse(response.isAcknowledged))
                }

                override fun onFailure(t: Exception) {
                    log.error("failed to update settings on index ${request.indexName}", t)
                    listener.onFailure(t)
                }
        })
    }

    private fun resolveIndices(state: ClusterState?, request: UpdateMetadataRequest, iner: IndexNameExpressionResolver
            ): Array<Index> {
        try {
            return iner.concreteIndices(state, object : IndicesRequest {
                override fun indices(): Array<String> {
                    return arrayOf(request.indexName)
                }

                override fun indicesOptions(): IndicesOptions {
                    return indicesOptions
                }
            })
        } catch (ex: IndexNotFoundException) {
            log.error("Failed to execute UpdateMetadataRequest. Index ${request.indexName} not found. type: ${request.type}: ${ex.stackTraceToString()}")
            throw ex
        }
    }

    private fun performMappingUpdate(concreteIndices: Array<Index>, request: UpdateMetadataRequest,
                                     listener: ActionListener<AcknowledgedResponse>, metadataMappingService: MetadataMappingService
    ) {
        val mappingRequest = request.request as PutMappingRequest
        val updateRequest = PutMappingClusterStateUpdateRequest(mappingRequest.source())
            .ackTimeout(mappingRequest.timeout()).clusterManagerNodeTimeout(mappingRequest.clusterManagerNodeTimeout())
            .indices(concreteIndices)

        metadataMappingService.putMapping(updateRequest,
            object : ActionListener<ClusterStateUpdateResponse> {
                override fun onResponse(response: ClusterStateUpdateResponse) {
                    listener.onResponse(AcknowledgedResponse(response.isAcknowledged))
                }
                override fun onFailure(ex: Exception) {
                    log.error("failed to put mappings on indices ${request.indexName} : ${ex.stackTraceToString()}")
                    listener.onFailure(ex)
                }
            })
    }
    private fun concreteAliases(
        action: AliasActions,
        metadata: Metadata,
        concreteIndex: String
    ): Array<String> {
        return if (action.expandAliasesWildcards()) {
            //for DELETE we expand the aliases
            val indexAsArray = arrayOf(concreteIndex)
            val aliasMetadata = metadata.findAliases(action, indexAsArray)
            val finalAliases: MutableList<String> = ArrayList()
            for (curAliases in aliasMetadata.values) {
                for (aliasMeta in curAliases) {
                    finalAliases.add(aliasMeta.alias())
                }
            }
            finalAliases.toTypedArray()
        } else {
            //for ADD and REMOVE_INDEX we just return the current aliases
            action.aliases()
        }
    }

    override fun clusterManagerOperation(request: UpdateMetadataRequest?, state: ClusterState?, listener: ActionListener<AcknowledgedResponse>?) {
        throw UnsupportedOperationException("The task parameter is required")
    }

}
