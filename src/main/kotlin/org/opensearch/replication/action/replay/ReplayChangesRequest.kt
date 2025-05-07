/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.replication.action.replay

import org.opensearch.action.support.replication.ReplicatedWriteRequest
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.index.shard.ShardId
import org.opensearch.index.translog.Translog

class ReplayChangesRequest : ReplicatedWriteRequest<ReplayChangesRequest> {

    val leaderAlias: String
    val leaderIndex: String
    val changes: List<Translog.Operation>
    val maxSeqNoOfUpdatesOrDeletes: Long

    constructor(
        shardId: ShardId,
        changes: List<Translog.Operation>,
        maxSeqNoOfUpdatesOrDeletes: Long,
        leaderAlias: String,
        leaderIndex: String,
    ) : super(shardId) {
        this.changes = changes
        this.maxSeqNoOfUpdatesOrDeletes = maxSeqNoOfUpdatesOrDeletes
        this.leaderAlias = leaderAlias
        this.leaderIndex = leaderIndex
    }

    constructor(inp: StreamInput) : super(inp) {
        leaderAlias = inp.readString()
        leaderIndex = inp.readString()
        changes = inp.readList(Translog.Operation::readOperation)
        maxSeqNoOfUpdatesOrDeletes = inp.readLong()
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(leaderAlias)
        out.writeString(leaderIndex)
        out.writeCollection(changes, Translog.Operation::writeOperation)
        out.writeLong(maxSeqNoOfUpdatesOrDeletes)
    }

    override fun toString(): String {
        return "ReplayChangesRequest[changes=<${changes.take(3)}]"
    }
}
