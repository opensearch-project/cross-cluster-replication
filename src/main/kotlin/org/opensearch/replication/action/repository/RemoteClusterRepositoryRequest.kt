/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.replication.action.repository

import org.opensearch.action.support.single.shard.SingleShardRequest
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.index.shard.ShardId
import org.opensearch.transport.RemoteClusterAwareRequest

abstract class RemoteClusterRepositoryRequest<T : SingleShardRequest<T>?> :
    SingleShardRequest<T>, RemoteClusterAwareRequest {

    val restoreUUID: String
    val node: DiscoveryNode
    val leaderShardId: ShardId
    val followerCluster: String
    val followerShardId: ShardId

    constructor(
        restoreUUID: String,
        node: DiscoveryNode,
        leaderShardId: ShardId,
        followerCluster: String,
        followerShardId: ShardId,
    ) : super(leaderShardId.indexName) {
        this.restoreUUID = restoreUUID
        this.node = node
        this.leaderShardId = leaderShardId
        this.followerCluster = followerCluster
        this.followerShardId = followerShardId
    }

    constructor(input: StreamInput) {
        restoreUUID = input.readString()
        node = DiscoveryNode(input)
        leaderShardId = ShardId(input)
        followerCluster = input.readString()
        followerShardId = ShardId(input)
        super.index = leaderShardId.indexName
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(restoreUUID)
        node.writeTo(out)
        leaderShardId.writeTo(out)
        out.writeString(followerCluster)
        followerShardId.writeTo(out)
    }

    override fun getPreferredTargetNode(): DiscoveryNode {
        return node
    }
}
