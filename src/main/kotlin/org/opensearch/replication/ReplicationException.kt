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

package org.opensearch.replication

import org.opensearch.OpenSearchException
import org.opensearch.action.ShardOperationFailedException
import org.opensearch.cluster.metadata.IndexMetadata.INDEX_UUID_NA_VALUE
import org.opensearch.index.shard.ShardId

/**
 * Base class replication exceptions. Note: Replication process may throw exceptions that do not derive from this such as
 * [org.opensearch.ResourceAlreadyExistsException], [org.opensearch.index.IndexNotFoundException] or
 * [org.opensearch.index.shard.ShardNotFoundException].
 */
class ReplicationException: OpenSearchException {

    constructor(message: String, vararg args: Any) : super(message, *args)

    constructor(message: String, cause: Throwable, vararg args: Any) : super(message,  cause, *args)

    constructor(message: String, shardFailures: Array<ShardOperationFailedException>) : super(message) {
        shardFailures.firstOrNull()?.let {
            setShard(ShardId(it.index(), INDEX_UUID_NA_VALUE, it.shardId()))
            // Add first failure as cause and rest as suppressed...
            initCause(it.cause)
            shardFailures.drop(1).forEach { f -> addSuppressed(f.cause) }
        }
    }
}
