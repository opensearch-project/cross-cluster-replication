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

package com.amazon.elasticsearch.replication

import org.elasticsearch.ElasticsearchException
import org.elasticsearch.action.ShardOperationFailedException
import org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_UUID_NA_VALUE
import org.elasticsearch.index.shard.ShardId

/**
 * Base class replication exceptions. Note: Replication process may throw exceptions that do not derive from this such as
 * [org.elasticsearch.ResourceAlreadyExistsException], [org.elasticsearch.index.IndexNotFoundException] or
 * [org.elasticsearch.index.shard.ShardNotFoundException].
 */
class ReplicationException: ElasticsearchException {

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
