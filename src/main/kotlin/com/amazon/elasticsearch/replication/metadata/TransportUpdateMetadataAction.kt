package com.amazon.elasticsearch.replication.metadata

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.message.ParameterizedMessage
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.IndicesRequest
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesClusterStateUpdateRequest
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions
import org.elasticsearch.action.admin.indices.close.CloseIndexClusterStateUpdateRequest
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingClusterStateUpdateRequest
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest
import org.elasticsearch.action.admin.indices.open.OpenIndexClusterStateUpdateRequest
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsClusterStateUpdateRequest
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.support.master.TransportMasterNodeAction
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse
import org.elasticsearch.cluster.ack.OpenIndexClusterStateUpdateResponse
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.block.ClusterBlockLevel
import org.elasticsearch.cluster.metadata.*
import org.elasticsearch.cluster.metadata.AliasAction.RemoveIndex
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.index.Index
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.rest.action.admin.indices.AliasesNotFoundException
import org.elasticsearch.tasks.Task
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService
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
) : TransportMasterNodeAction<UpdateMetadataRequest, AcknowledgedResponse>(UpdateMetadataAction.NAME,
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

    override fun masterOperation(
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
                .ackTimeout(request.timeout()).masterNodeTimeout(openIndexRequest.masterNodeTimeout())
                .indices(concreteIndices).waitForActiveShards(openIndexRequest.waitForActiveShards())

        indexStateService.openIndex(updateRequest, object : ActionListener<OpenIndexClusterStateUpdateResponse> {
            override fun onResponse(response: OpenIndexClusterStateUpdateResponse) {
                listener.onResponse(OpenIndexResponse(response.isAcknowledged, response.isShardsAcknowledged))
            }

            override fun onFailure(t: java.lang.Exception) {
                log.error({ ParameterizedMessage("failed to open indices [{}]", concreteIndices as Any) }, t)
                listener.onFailure(t)
            }
        })
    }

    private fun performCloseIndex(task :Task , concreteIndices: Array<Index>, request: UpdateMetadataRequest,
                                 listener: ActionListener<AcknowledgedResponse>) {
        val request = request.request as CloseIndexRequest
        val closeRequest = CloseIndexClusterStateUpdateRequest(task.id)
                .ackTimeout(request.timeout())
                .masterNodeTimeout(request.masterNodeTimeout())
                .waitForActiveShards(request.waitForActiveShards())
                .indices(concreteIndices)

        indexStateService.closeIndices(closeRequest, object : ActionListener<CloseIndexResponse> {
            override fun onResponse(response: CloseIndexResponse) {
                listener.onResponse(response)
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
                .ackTimeout(request.timeout()).masterNodeTimeout(request.masterNodeTimeout())

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
            .masterNodeTimeout(request.masterNodeTimeout())


        updateSettingsService.updateSettings(clusterStateUpdateRequest,
            object : ActionListener<ClusterStateUpdateResponse> {
                override fun onResponse(response: ClusterStateUpdateResponse) {
                    listener.onResponse(AcknowledgedResponse(response.isAcknowledged))
                }

                override fun onFailure(t: Exception) {
                    log.error("failed to update settings on index ${request.indexName}")
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
            log.error("Failed to execute UpdateMetadataRequest. Index ${request.indexName} not found. type: ${request.type}: $ex")
            throw ex
        }
    }

    private fun performMappingUpdate(concreteIndices: Array<Index>, request: UpdateMetadataRequest,
                                     listener: ActionListener<AcknowledgedResponse>, metadataMappingService: MetadataMappingService
    ) {
        val mappingRequest = request.request as PutMappingRequest
        val updateRequest = PutMappingClusterStateUpdateRequest()
            .ackTimeout(mappingRequest.timeout()).masterNodeTimeout(mappingRequest.masterNodeTimeout())
            .indices(concreteIndices).type(mappingRequest.type())
            .source(mappingRequest.source())

        metadataMappingService.putMapping(updateRequest,
            object : ActionListener<ClusterStateUpdateResponse> {
                override fun onResponse(response: ClusterStateUpdateResponse) {
                    listener.onResponse(AcknowledgedResponse(response.isAcknowledged))
                }
                override fun onFailure(ex: Exception) {
                    log.error("failed to put mappings on indices ${request.indexName} : $ex")
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
            for (curAliases in aliasMetadata.values()) {
                for (aliasMeta in curAliases.value) {
                    finalAliases.add(aliasMeta.alias())
                }
            }
            finalAliases.toTypedArray()
        } else {
            //for ADD and REMOVE_INDEX we just return the current aliases
            action.aliases()
        }
    }

    override fun masterOperation(request: UpdateMetadataRequest?, state: ClusterState?, listener: ActionListener<AcknowledgedResponse>?) {
        throw UnsupportedOperationException("The task parameter is required")
    }

}