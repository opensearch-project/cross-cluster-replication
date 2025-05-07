/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.replication.metadata.store

import org.opensearch.index.seqno.SequenceNumbers

data class UpdateReplicationMetadataRequest(
    val replicationMetadata: ReplicationMetadata,
    val ifSeqno: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val ifPrimaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
)
