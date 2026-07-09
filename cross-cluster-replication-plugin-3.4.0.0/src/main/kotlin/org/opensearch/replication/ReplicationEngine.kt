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

package org.opensearch.replication

import org.opensearch.index.engine.EngineConfig
import org.opensearch.index.engine.InternalEngine
import org.opensearch.index.seqno.SequenceNumbers

class ReplicationEngine(config: EngineConfig) : InternalEngine(config) {

    override fun assertPrimaryIncomingSequenceNumber(origin: Operation.Origin, seqNo: Long): Boolean {
        assert(origin == Operation.Origin.PRIMARY) { "Expected origin PRIMARY for replicated ops but was $origin" }
        assert(seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) { "Expected valid sequence number for replicated op but was unassigned" }
        return true
    }

    override fun generateSeqNoForOperationOnPrimary(operation: Operation): Long {
        check(operation.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) { "Expected valid sequence number for replicate op but was unassigned"}
        return operation.seqNo()
    }

    override fun indexingStrategyForOperation(index: Index): IndexingStrategy {
        return planIndexingAsNonPrimary(index)
    }

    override fun deletionStrategyForOperation(delete: Delete): DeletionStrategy {
        return planDeletionAsNonPrimary(delete)
    }

    override fun assertNonPrimaryOrigin(operation: Operation): Boolean {
        return true
    }
}
