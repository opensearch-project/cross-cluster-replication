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

package org.opensearch.replication.action.repository

import org.opensearch.action.support.single.shard.SingleShardRequest
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.index.shard.ShardId
import org.opensearch.transport.RemoteClusterAwareRequest

abstract class RemoteClusterRepositoryRequest<T : SingleShardRequest<T>?>:
        SingleShardRequest<T>, RemoteClusterAwareRequest {

    val restoreUUID: String
    val node: DiscoveryNode
    val leaderShardId: ShardId
    val followerCluster: String
    val followerShardId: ShardId

    constructor(restoreUUID: String,
                node: DiscoveryNode,
                leaderShardId: ShardId,
                followerCluster: String,
                followerShardId: ShardId): super(leaderShardId.indexName) {
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
