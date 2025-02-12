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

package org.opensearch.replication.metadata.store

import org.opensearch.replication.util.execute
import org.opensearch.replication.util.suspending
import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.ResourceAlreadyExistsException
import org.opensearch.ResourceNotFoundException
import org.opensearch.action.admin.cluster.health.ClusterHealthAction
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest
import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.transport.client.Client
import org.opensearch.cluster.health.ClusterHealthStatus
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.lifecycle.AbstractLifecycleComponent
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.common.xcontent.XContentType
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.replication.util.suspendExecuteWithRetries

class ReplicationMetadataStore constructor(val client: Client, val clusterService: ClusterService,
                               val namedXContentRegistry: NamedXContentRegistry): AbstractLifecycleComponent() {

    companion object {
        const val REPLICATION_CONFIG_SYSTEM_INDEX = ".replication-metadata-store"
        const val MAPPING_TYPE = "_doc"
        const val MAPPING_META = "_meta"
        const val MAPPING_SCHEMA_VERSION = "schema_version"
        const val DEFAULT_SCHEMA_VERSION = 1
        val REPLICATION_CONFIG_SYSTEM_INDEX_MAPPING = ReplicationMetadataStore::class.java
            .classLoader.getResource("mappings/replication-metadata-store.json")!!.readText()
        var REPLICATION_STORE_MAPPING_VERSION: Int
        init {
            REPLICATION_STORE_MAPPING_VERSION = getSchemaVersion(REPLICATION_CONFIG_SYSTEM_INDEX_MAPPING)
        }
        private val log = LogManager.getLogger(ReplicationMetadataStore::class.java)


        private fun getSchemaVersion(mapping: String): Int {
            val xcp = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE, mapping)

            while (!xcp.isClosed) {
                val token = xcp.currentToken()
                if (token != null && token != XContentParser.Token.END_OBJECT && token != XContentParser.Token.START_OBJECT) {
                    if (xcp.currentName() != MAPPING_META) {
                        xcp.nextToken()
                        xcp.skipChildren()
                    } else {
                        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                            when (xcp.currentName()) {
                                MAPPING_SCHEMA_VERSION -> {
                                    val version = xcp.intValue()
                                    require(version > -1)
                                    return version
                                }
                                else -> xcp.nextToken()
                            }
                        }
                    }
                }
                xcp.nextToken()
            }
            return DEFAULT_SCHEMA_VERSION
        }
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

        checkAndWaitForStoreHealth()
        checkAndUpdateMapping()

        val id = getId(addReq.replicationMetadata.metadataType, addReq.replicationMetadata.connectionName,
                addReq.replicationMetadata.followerContext.resource)
        val indexReqBuilder = client.prepareIndex(REPLICATION_CONFIG_SYSTEM_INDEX).setId(id)
                .setSource(addReq.replicationMetadata.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
        return client.suspending(indexReqBuilder::execute, defaultContext = true)("replication")
    }

    private suspend fun checkAndUpdateMapping() {
        var currentSchemaVersion = DEFAULT_SCHEMA_VERSION
        val indexMetadata = clusterService.state().metadata.indices.getOrDefault(REPLICATION_CONFIG_SYSTEM_INDEX, null)
                ?: throw ResourceNotFoundException("Index metadata doesn't exist for $REPLICATION_CONFIG_SYSTEM_INDEX")
        val mappingMetadata = indexMetadata.mapping()?.sourceAsMap?.get(MAPPING_META)
        if(mappingMetadata != null && mappingMetadata is HashMap<*,*>) {
            currentSchemaVersion = mappingMetadata.get(MAPPING_SCHEMA_VERSION) as Int
        }

        if(REPLICATION_STORE_MAPPING_VERSION > currentSchemaVersion) {
            val putMappingReq = PutMappingRequest(REPLICATION_CONFIG_SYSTEM_INDEX)
                    .source(REPLICATION_CONFIG_SYSTEM_INDEX_MAPPING, XContentType.JSON)
            val putMappingRes = client.suspending(client.admin().indices()::putMapping, defaultContext = true)(putMappingReq)
            if(!putMappingRes.isAcknowledged) {
                log.error("Mapping update failed for replication store - $REPLICATION_CONFIG_SYSTEM_INDEX")
            }
        }
    }

    suspend fun getMetadata(getMetadataReq: GetReplicationMetadataRequest,
                            fetch_from_primary: Boolean): GetReplicationMetadataResponse {
        val id = getId(getMetadataReq.metadataType, getMetadataReq.connectionName, getMetadataReq.resourceName)

        if(!configStoreExists()) {
            throw ResourceNotFoundException("Metadata for $id doesn't exist")
        }

        checkAndWaitForStoreHealth()

        val getReq = GetRequest(REPLICATION_CONFIG_SYSTEM_INDEX, id)
        getReq.realtime(true)
        getReq.refresh(true)
        if(fetch_from_primary) {
            val preference = getPreferenceOnPrimaryNode() ?: throw IllegalStateException("Primary shard to fetch id[$id] in index[$REPLICATION_CONFIG_SYSTEM_INDEX] doesn't exist")
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

        val clusterHealthReq = ClusterHealthRequest(REPLICATION_CONFIG_SYSTEM_INDEX).waitForYellowStatus()
        val clusterHealthRes = client.admin().cluster().health(clusterHealthReq).actionGet(timeout)
        assert(clusterHealthRes.status <= ClusterHealthStatus.YELLOW) { "Replication metadata store is unhealthy" }

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

        checkAndWaitForStoreHealth()

        val delReq = DeleteRequest(REPLICATION_CONFIG_SYSTEM_INDEX, id)
        return client.suspending(client::delete, defaultContext = true)(delReq)
    }

    suspend fun updateMetadata(updateMetadataReq: UpdateReplicationMetadataRequest): IndexResponse {
        val metadata = updateMetadataReq.replicationMetadata
        val id = getId(metadata.metadataType, metadata.connectionName, metadata.followerContext.resource)
        if(!configStoreExists()) {
            throw ResourceNotFoundException("Metadata for $id doesn't exist")
        }
        checkAndWaitForStoreHealth()
        checkAndUpdateMapping()

        val indexReqBuilder = client.prepareIndex(REPLICATION_CONFIG_SYSTEM_INDEX).setId(id)
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
                .mapping(REPLICATION_CONFIG_SYSTEM_INDEX_MAPPING, XContentType.JSON)
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

    private suspend fun checkAndWaitForStoreHealth() {
        if(!configStoreExists()) {
            return
        }
        val clusterHealthReq = ClusterHealthRequest(REPLICATION_CONFIG_SYSTEM_INDEX).waitForYellowStatus()
        // This should ensure that security plugin and shards are active during boot-up before triggering the requests
        client.suspendExecuteWithRetries(null, ClusterHealthAction.INSTANCE,
                clusterHealthReq, log=log, defaultContext = true)
    }
}
