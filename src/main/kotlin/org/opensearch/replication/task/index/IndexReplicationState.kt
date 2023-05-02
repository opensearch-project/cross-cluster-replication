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

package org.opensearch.replication.task.index

import org.opensearch.replication.task.ReplicationState
import org.opensearch.replication.task.shard.ShardReplicationParams
import org.opensearch.core.ParseField
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ObjectParser
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.index.shard.ShardId
import org.opensearch.persistent.PersistentTaskState
import org.opensearch.persistent.PersistentTasksCustomMetadata.PersistentTask
import java.io.IOException
import java.lang.IllegalArgumentException

sealed class IndexReplicationState : PersistentTaskState {
    var state: ReplicationState

    companion object {
        const val NAME = IndexReplicationExecutor.TASK_NAME

        fun reader(inp : StreamInput) : IndexReplicationState {
            val state = inp.readEnum(ReplicationState::class.java)!!
            return when (state) {
                ReplicationState.INIT -> InitialState
                ReplicationState.RESTORING -> RestoreState
                ReplicationState.INIT_FOLLOW -> InitFollowState
                ReplicationState.FOLLOWING -> FollowingState(inp)
                ReplicationState.COMPLETED -> CompletedState
                ReplicationState.MONITORING -> MonitoringState
                ReplicationState.FAILED -> FailedState(inp)
            }
        }

        private val PARSER = ObjectParser<Builder, Void>(NAME, true) { Builder() }

        init {
            PARSER.declareString(Builder::setIndexTaskState, ParseField("state"))
        }

        @Throws(IOException::class)
        fun fromXContent(parser: XContentParser): IndexReplicationState {
            return PARSER.parse(parser, null).build()
        }
    }

    constructor(state: ReplicationState) {
        this.state = state
    }

    override fun writeTo(out: StreamOutput) {
        out.writeEnum(state)
    }

    final override fun getWriteableName(): String = NAME

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
        return builder.startObject()
            .field("state", state)
            .endObject()
    }

    class Builder {
        lateinit var state: String

        fun setIndexTaskState(state: String) {
            this.state = state
        }

        fun build(): IndexReplicationState {
            // Issue details - https://github.com/opensearch-project/cross-cluster-replication/issues/223
            state = if(!this::state.isInitialized) {
                ReplicationState.MONITORING.name
            } else {
                state
            }
            return when (state) {
                ReplicationState.INIT.name -> InitialState
                ReplicationState.RESTORING.name -> RestoreState
                ReplicationState.INIT_FOLLOW.name -> InitFollowState
                ReplicationState.FOLLOWING.name -> FollowingState(mapOf())
                ReplicationState.COMPLETED.name -> CompletedState
                ReplicationState.MONITORING.name -> MonitoringState
                ReplicationState.FAILED.name -> FailedState(mapOf(), "")
                else -> throw IllegalArgumentException("$state - Not a valid state for index replication task")
            }
        }
    }
}

/**
 * Singleton that represent initial state.
 */
object InitialState : IndexReplicationState(ReplicationState.INIT)

/**
 * Singleton that represents an in-progress restore.
 */
object RestoreState : IndexReplicationState(ReplicationState.RESTORING)

/**
 * Singleton that represents initial follow.
 */
object InitFollowState : IndexReplicationState(ReplicationState.INIT_FOLLOW)

/**
 * Singleton that represents completed task state.
 */
object CompletedState : IndexReplicationState(ReplicationState.COMPLETED)

/**
 * Singleton that represents monitoring state.
 */
object MonitoringState : IndexReplicationState(ReplicationState.MONITORING)

/**
 * State when index task is in failed state.
 */
data class FailedState(val failedShards: Map<ShardId, PersistentTask<ShardReplicationParams>>, val errorMsg: String)
    : IndexReplicationState(ReplicationState.FAILED) {
    constructor(inp: StreamInput) : this(inp.readMap(::ShardId, ::PersistentTask), "")

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeMap(failedShards, { o, k -> k.writeTo(o) }, { o, v -> v.writeTo(o) })
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
        return builder.startObject()
                .field("error_message", errorMsg)
                .field("failed_shard_replication_tasks").map(failedShards.mapKeys { it.key.toString() })
                .field("state", state)
                .endObject()
    }
}

/**
 * State when index is being actively replicated.
 */
data class FollowingState(val shardReplicationTasks: Map<ShardId, PersistentTask<ShardReplicationParams>>?)
    : IndexReplicationState(ReplicationState.FOLLOWING) {

    constructor(inp: StreamInput) : this(inp.readMap(::ShardId, ::PersistentTask))

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeMap(shardReplicationTasks, { o, k -> k.writeTo(o) }, { o, v -> v.writeTo(o) })
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
        return builder.startObject()
            .field("shard_replication_tasks").map(shardReplicationTasks?.mapKeys { it.key.toString() })
            .field("state", state)
            .endObject()
    }
}
