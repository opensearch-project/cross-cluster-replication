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

package org.opensearch.replication.action.replay

import org.opensearch.action.support.replication.ReplicatedWriteRequest
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.index.shard.ShardId
import org.opensearch.index.translog.Translog

class ReplayChangesRequest : ReplicatedWriteRequest<ReplayChangesRequest> {

    val remoteCluster: String
    val remoteIndex: String
    val changes: List<Translog.Operation>
    val maxSeqNoOfUpdatesOrDeletes: Long

    constructor(shardId: ShardId,
                changes: List<Translog.Operation>,
                maxSeqNoOfUpdatesOrDeletes: Long,
                remoteCluster: String,
                remoteIndex: String) : super(shardId) {
        this.changes = changes
        this.maxSeqNoOfUpdatesOrDeletes = maxSeqNoOfUpdatesOrDeletes
        this.remoteCluster = remoteCluster
        this.remoteIndex = remoteIndex
    }

    constructor(inp: StreamInput) : super(inp) {
        remoteCluster = inp.readString()
        remoteIndex = inp.readString()
        changes = inp.readList(Translog.Operation::readOperation)
        maxSeqNoOfUpdatesOrDeletes = inp.readLong()
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(remoteCluster)
        out.writeString(remoteIndex)
        out.writeCollection(changes, Translog.Operation::writeOperation)
        out.writeLong(maxSeqNoOfUpdatesOrDeletes)
    }

    override fun toString(): String {
        return "ReplayChangesRequest[changes=<${changes.take(3)}]"
    }
}