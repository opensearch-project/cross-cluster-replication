/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.replication.action.repository

import org.opensearch.action.ActionRequestValidationException
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.index.shard.ShardId

class ReleaseLeaderResourcesRequest : RemoteClusterRepositoryRequest<ReleaseLeaderResourcesRequest> {

    constructor(
        restoreUUID: String,
        node: DiscoveryNode,
        leaderShardId: ShardId,
        followerCluster: String,
        followerShardId: ShardId,
    ) :
        super(restoreUUID, node, leaderShardId, followerCluster, followerShardId)

    constructor(input: StreamInput) : super(input)

    override fun validate(): ActionRequestValidationException? {
        return null
    }
}
