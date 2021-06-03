package com.amazon.elasticsearch.replication.metadata.store

import com.amazon.elasticsearch.replication.util.execute
import com.amazon.elasticsearch.replication.util.suspending
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.ResourceAlreadyExistsException
import org.elasticsearch.ResourceNotFoundException
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.component.AbstractLifecycleComponent
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.inject.Singleton
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentType


@Singleton
class ReplicationMetadataStore @Inject constructor(val client: Client, val clusterService: ClusterService,
                               val namedXContentRegistry: NamedXContentRegistry): AbstractLifecycleComponent() {

    companion object {
        const val REPLICATION_CONFIG_SYSTEM_INDEX = ".replication-metadata-store"
        const val MAPPING_TYPE = "_doc"
        val REPLICATION_CONFIG_SYSTEM_INDEX_MAPPING = javaClass.classLoader.getResource("mappings/replication-metadata-store.json").readText()
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

        return suspending(indexReqBuilder::execute)("replication")
    }

    suspend fun getMetadata(getMetadataReq: GetReplicationMetadataRequest): ReplicationMetadata {
        val id = getId(getMetadataReq.metadataType, getMetadataReq.connectionName, getMetadataReq.resourceName)

        if(!configStoreExists()) {
            throw ResourceNotFoundException("Metadata for $id doesn't exist")
        }

        // TODO: Specify routing to fetch the metadata from primary shard
        val getReq = GetRequest(REPLICATION_CONFIG_SYSTEM_INDEX, id)

        val getRes = suspending(client::get)(getReq)
        val parser = XContentHelper.createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE,
                getRes.sourceAsBytesRef, XContentType.JSON)
        return ReplicationMetadata.fromXContent(parser)
    }

    suspend fun deleteMetadata(delMetadataReq: DeleteReplicationMetadataRequest): DeleteResponse {
        val id = getId(delMetadataReq.metadataType, delMetadataReq.connectionName, delMetadataReq.resourceName)
        if(!configStoreExists()) {
            throw ResourceNotFoundException("Metadata for $id doesn't exist")
        }

        val delReq = DeleteRequest(REPLICATION_CONFIG_SYSTEM_INDEX, id)
        return suspending(client::delete)(delReq)
    }

    suspend fun updateMetadata(updateMetadataReq: UpdateReplicationMetadataRequest): UpdateResponse {
        val metadata = updateMetadataReq.replicationMetadata
        val id = getId(metadata.metadataType, metadata.connectionName, metadata.followerContext.resource)
        if(!configStoreExists()) {
            throw ResourceNotFoundException("Metadata for $id doesn't exist")
        }
        // TODO: Check and update if existing index needs mapping update

        val updateReq = UpdateRequest(REPLICATION_CONFIG_SYSTEM_INDEX, id)
                .setIfSeqNo(updateMetadataReq.ifSeqno)
                .setIfPrimaryTerm(updateMetadataReq.ifPrimaryTerm)
                .doc(metadata.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
        return suspending(client::update)(updateReq)
    }

    private fun getId(metadataType: String, connectionName: String, resourceName: String): String {
        var id = resourceName
        if(metadataType == ReplicationStoreMetadataType.AUTO_FOLLOW.name)
            id = "${connectionName}:${resourceName}"
        return id
    }

    private suspend fun createIndex(): CreateIndexResponse {
        val createIndexReq = CreateIndexRequest(REPLICATION_CONFIG_SYSTEM_INDEX, configStoreSettings())
                .mapping(MAPPING_TYPE, REPLICATION_CONFIG_SYSTEM_INDEX_MAPPING, XContentType.JSON)
        return suspending(client.admin().indices()::create)(createIndexReq)
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
