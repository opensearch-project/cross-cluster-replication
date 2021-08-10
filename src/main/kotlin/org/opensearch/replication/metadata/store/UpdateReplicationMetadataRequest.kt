package org.opensearch.replication.metadata.store

import org.opensearch.index.seqno.SequenceNumbers

data class UpdateReplicationMetadataRequest(val replicationMetadata: ReplicationMetadata,
                                       val ifSeqno: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
                                       val ifPrimaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
}
