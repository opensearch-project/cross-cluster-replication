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

package org.opensearch.replication.action.changes

import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.support.single.shard.SingleShardRequest
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.index.shard.ShardId
import org.opensearch.transport.RemoteClusterAwareRequest

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
