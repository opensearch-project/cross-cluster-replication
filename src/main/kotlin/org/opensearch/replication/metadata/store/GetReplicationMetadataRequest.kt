package org.opensearch.replication.metadata.store

data class GetReplicationMetadataRequest(val metadataType: String,
                                    val connectionName: String?,
                                    val resourceName: String) {
}
