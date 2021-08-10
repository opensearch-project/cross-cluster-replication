package org.opensearch.replication.metadata.store

import org.opensearch.replication.util.execute
import org.opensearch.replication.util.suspending
import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.ResourceAlreadyExistsException
import org.opensearch.ResourceNotFoundException
import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.component.AbstractLifecycleComponent
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.common.xcontent.*


class ReplicationMetadataStore constructor(val client: Client, val clusterService: ClusterService,
                               val namedXContentRegistry: NamedXContentRegistry): AbstractLifecycleComponent() {

    companion object {
        const val REPLICATION_CONFIG_SYSTEM_INDEX = ".replication-metadata-store"
        const val MAPPING_TYPE = "_doc"
        val REPLICATION_CONFIG_SYSTEM_INDEX_MAPPING = javaClass.classLoader.getResource("mappings/replication-metadata-store.json").readText()
        private val log = LogManager.getLogger(ReplicationMetadataStore::class.java)
    }

    suspend fun addMetadata(addReq: AddReplicationMetadataRequest): IndexResponse {
        if(!configStoreExists()) {
            try {
                createIndex()
            } catch (ex: Exception) {
                if (ExceptionsHelper.unwrapCause(ex) !is ResourceAlreadyExistsException) {
                    throw ex
                }
            }
        }
        // TODO: Check and update if existing index needs mapping update

        val id = getId(addReq.replicationMetadata.metadataType, addReq.replicationMetadata.connectionName,
                addReq.replicationMetadata.followerContext.resource)
        val indexReqBuilder = client.prepareIndex(REPLICATION_CONFIG_SYSTEM_INDEX, MAPPING_TYPE, id)
                .setSource(addReq.replicationMetadata.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
        return client.suspending(indexReqBuilder::execute, defaultContext = true)("replication")
    }

    suspend fun getMetadata(getMetadataReq: GetReplicationMetadataRequest,
                            fetch_from_primary: Boolean): GetReplicationMetadataResponse {
        val id = getId(getMetadataReq.metadataType, getMetadataReq.connectionName, getMetadataReq.resourceName)

        if(!configStoreExists()) {
            throw ResourceNotFoundException("Metadata for $id doesn't exist")
        }

        val getReq = GetRequest(REPLICATION_CONFIG_SYSTEM_INDEX, id)
        getReq.realtime(true)
        getReq.refresh(true)
        if(fetch_from_primary) {
            val preference = getPreferenceOnPrimaryNode() ?: throw throw IllegalStateException("Primary shard to fetch id[$id] in index[$REPLICATION_CONFIG_SYSTEM_INDEX] doesn't exist")
            getReq.preference(preference)
        }

        val getRes = client.suspending(client::get, defaultContext = true)(getReq)
        if(getRes.sourceAsBytesRef == null) {
            throw ResourceNotFoundException("Metadata for $id doesn't exist")
        }
        val parser = XContentHelper.createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE,
                getRes.sourceAsBytesRef, XContentType.JSON)
        return GetReplicationMetadataResponse(ReplicationMetadata.fromXContent(parser), getRes.seqNo, getRes.primaryTerm)
    }

    fun getMetadata(getMetadataReq: GetReplicationMetadataRequest,
                    fetch_from_primary: Boolean,
                    timeout: Long): GetReplicationMetadataResponse {
        val id = getId(getMetadataReq.metadataType, getMetadataReq.connectionName, getMetadataReq.resourceName)

        if(!configStoreExists()) {
            throw ResourceNotFoundException("Metadata for $id doesn't exist")
        }

        val getReq = GetRequest(REPLICATION_CONFIG_SYSTEM_INDEX, id)
        getReq.realtime(true)
        getReq.refresh(true)
        if(fetch_from_primary) {
            val preference = getPreferenceOnPrimaryNode() ?: throw IllegalStateException("Primary shard to fetch id[$id] in index[$REPLICATION_CONFIG_SYSTEM_INDEX] doesn't exist")
            getReq.preference(preference)
        }

        var storedContext: ThreadContext.StoredContext? = null
        try {
            storedContext = client.threadPool().threadContext.stashContext()
            val getRes = client.get(getReq).actionGet(timeout)
            if(getRes.sourceAsBytesRef == null) {
                throw ResourceNotFoundException("Metadata for $id doesn't exist")
            }
            val parser = XContentHelper.createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE,
                    getRes.sourceAsBytesRef, XContentType.JSON)
            return GetReplicationMetadataResponse(ReplicationMetadata.fromXContent(parser), getRes.seqNo, getRes.primaryTerm)
        } finally {
            storedContext?.close()
        }

    }

    /**
     * Preference to set for the getMetadata requests
     * - Fetch from the primary shards
     */
    private fun getPreferenceOnPrimaryNode(): String? {
        // Only one primary shard for the store
        clusterService.state().routingTable
            .activePrimaryShardsGrouped(arrayOf(REPLICATION_CONFIG_SYSTEM_INDEX), false).forEach {
                val shardRouting = it.shardRoutings.firstOrNull { shardRouting ->
                    shardRouting.currentNodeId() != null
                }
                if(shardRouting != null) {
                    log.debug("_only_nodes to fetch metdata[$REPLICATION_CONFIG_SYSTEM_INDEX] - ${shardRouting.currentNodeId()}")
                    return "_only_nodes:${shardRouting.currentNodeId()}"
                }
            }
        return null
    }

    suspend fun deleteMetadata(delMetadataReq: DeleteReplicationMetadataRequest): DeleteResponse {
        val id = getId(delMetadataReq.metadataType, delMetadataReq.connectionName, delMetadataReq.resourceName)
        if(!configStoreExists()) {
            throw ResourceNotFoundException("Metadata for $id doesn't exist")
        }

        val delReq = DeleteRequest(REPLICATION_CONFIG_SYSTEM_INDEX, id)
        return client.suspending(client::delete, defaultContext = true)(delReq)
    }

    suspend fun updateMetadata(updateMetadataReq: UpdateReplicationMetadataRequest): IndexResponse {
        val metadata = updateMetadataReq.replicationMetadata
        val id = getId(metadata.metadataType, metadata.connectionName, metadata.followerContext.resource)
        if(!configStoreExists()) {
            throw ResourceNotFoundException("Metadata for $id doesn't exist")
        }
        // TODO: Check and update if existing index needs mapping update

        val indexReqBuilder = client.prepareIndex(REPLICATION_CONFIG_SYSTEM_INDEX, MAPPING_TYPE, id)
                .setSource(updateMetadataReq.replicationMetadata.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .setIfSeqNo(updateMetadataReq.ifSeqno)
                .setIfPrimaryTerm(updateMetadataReq.ifPrimaryTerm)
        return client.suspending(indexReqBuilder::execute, defaultContext = true)("replication")
    }

    private fun getId(metadataType: String, connectionName: String?, resourceName: String): String {
        var id = resourceName
        if(metadataType == ReplicationStoreMetadataType.AUTO_FOLLOW.name) {
            assert(connectionName != null)
            id = "${connectionName}:${resourceName}"
        }
        return id
    }

    private suspend fun createIndex(): CreateIndexResponse {
        val createIndexReq = CreateIndexRequest(REPLICATION_CONFIG_SYSTEM_INDEX, configStoreSettings())
                .mapping(MAPPING_TYPE, REPLICATION_CONFIG_SYSTEM_INDEX_MAPPING, XContentType.JSON)
        return client.suspending(client.admin().indices()::create, defaultContext = true)(createIndexReq)
    }

    private fun configStoreExists(): Boolean {
        return clusterService.state().routingTable().hasIndex(REPLICATION_CONFIG_SYSTEM_INDEX)
    }

    private fun configStoreSettings(): Settings {
        return Settings.builder()
                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.key, 1)
                .put(IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.key, "0-1")
                .put(IndexMetadata.INDEX_PRIORITY_SETTING.key, Int.MAX_VALUE)
                .put(IndexMetadata.INDEX_HIDDEN_SETTING.key, true)
                .build()
    }

    override fun doStart() {
    }

    override fun doStop() {
    }

    override fun doClose() {
    }
}
