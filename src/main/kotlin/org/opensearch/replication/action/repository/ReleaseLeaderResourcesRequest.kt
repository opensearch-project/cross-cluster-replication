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

import org.opensearch.action.ActionRequestValidationException
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.index.shard.ShardId

class ReleaseLeaderResourcesRequest: RemoteClusterRepositoryRequest<ReleaseLeaderResourcesRequest> {

    constructor(restoreUUID: String, node: DiscoveryNode, leaderShardId: ShardId,
                followerCluster: String, followerShardId: ShardId):
            super(restoreUUID, node, leaderShardId, followerCluster, followerShardId)

    constructor(input : StreamInput): super(input)

    override fun validate(): ActionRequestValidationException? {
        return null
    }
}
