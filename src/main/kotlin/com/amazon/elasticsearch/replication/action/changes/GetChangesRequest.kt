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

package com.amazon.elasticsearch.replication.action.changes

import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.support.single.shard.SingleShardRequest
import org.elasticsearch.cluster.node.DiscoveryNode
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.transport.RemoteClusterAwareRequest

class GetChangesRequest : SingleShardRequest<GetChangesRequest> {
    val shardId : ShardId
    val fromSeqNo: Long
    val toSeqNo: Long

    constructor(shardId: ShardId, fromSeqNo: Long, toSeqNo: Long) : super(shardId.indexName) {
        this.shardId = shardId
        this.fromSeqNo = fromSeqNo
        this.toSeqNo = toSeqNo
    }

    constructor(input : StreamInput) : super(input) {
        this.shardId = ShardId(input)
        this.fromSeqNo = input.readLong()
        this.toSeqNo = input.readVLong()
    }

    override fun validate(): ActionRequestValidationException? {
        return super.validateNonNullIndex()
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        shardId.writeTo(out)
        out.writeLong(fromSeqNo)
        out.writeVLong(toSeqNo)
    }
}
