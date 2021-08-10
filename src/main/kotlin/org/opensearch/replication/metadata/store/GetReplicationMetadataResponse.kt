package org.opensearch.replication.metadata.store

data class GetReplicationMetadataResponse(val replicationMetadata: ReplicationMetadata,
                                          val seqNo: Long,
                                          val primaryTerm: Long) {
}