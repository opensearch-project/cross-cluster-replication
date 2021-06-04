package com.amazon.elasticsearch.replication.metadata.store

data class DeleteReplicationMetadataRequest(val metadataType: String,
                                       val connectionName: String?,
                                       val resourceName: String) {
}
