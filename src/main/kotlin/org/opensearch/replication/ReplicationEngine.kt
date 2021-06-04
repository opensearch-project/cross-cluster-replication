/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
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
