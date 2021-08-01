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

package com.amazon.elasticsearch.replication.action.replay

import org.elasticsearch.action.support.replication.ReplicatedWriteRequest
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.index.translog.Translog

class ReplayChangesRequest : ReplicatedWriteRequest<ReplayChangesRequest> {

    val leaderAlias: String
    val leaderIndex: String
    val changes: List<Translog.Operation>
    val maxSeqNoOfUpdatesOrDeletes: Long

    constructor(shardId: ShardId,
                changes: List<Translog.Operation>,
                maxSeqNoOfUpdatesOrDeletes: Long,
                leaderAlias: String,
                leaderIndex: String) : super(shardId) {
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