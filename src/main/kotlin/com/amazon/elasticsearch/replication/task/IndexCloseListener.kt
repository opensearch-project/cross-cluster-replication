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

package com.amazon.elasticsearch.replication.task

import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.IndexService
import org.elasticsearch.index.shard.IndexEventListener
import org.elasticsearch.index.shard.IndexShard
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.indices.cluster.IndicesClusterStateService
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

object IndexCloseListener : IndexEventListener {

    private val tasks = ConcurrentHashMap<Any, MutableSet<CrossClusterReplicationTask>>()

    fun addCloseListener(indexOrShardId: Any, task: CrossClusterReplicationTask) {
        require(indexOrShardId is String || indexOrShardId is ShardId) {
            "Can't register a close listener for ${indexOrShardId}. Only Index or ShardIds are allowed."
        }
        tasks.computeIfAbsent(indexOrShardId) { Collections.synchronizedSet(mutableSetOf()) }.add(task)
    }

    fun removeCloseListener(indexOrShardId: Any, task: CrossClusterReplicationTask) {
        tasks.computeIfPresent(indexOrShardId) { _, v ->
            v.remove(task)
            if (v.isEmpty()) null else v
        }
    }

    override fun beforeIndexShardClosed(shardId: ShardId, indexShard: IndexShard?, indexSettings: Settings) {
        super.beforeIndexShardClosed(shardId, indexShard, indexSettings)
        val tasksToCancel = tasks.remove(shardId)
        if (tasksToCancel != null) {
            for (task in tasksToCancel) {
                task.onIndexShardClosed(shardId, indexShard, indexSettings)
            }
        }
    }

    override fun beforeIndexRemoved(indexService: IndexService,
                                    reason: IndicesClusterStateService.AllocatedIndices.IndexRemovalReason) {
        super.beforeIndexRemoved(indexService, reason)
        val tasksToCancel = tasks.remove(indexService.index().name)
        if (tasksToCancel != null) {
            for (task in tasksToCancel) {
                task.onIndexRemoved(indexService, reason)
            }
        }
    }
}