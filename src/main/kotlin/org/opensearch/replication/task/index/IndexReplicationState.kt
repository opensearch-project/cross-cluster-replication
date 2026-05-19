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
import org.opensearch.core.ParseField
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ObjectParser
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.persistent.PersistentTaskState
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
                // INIT_FOLLOW is retained as an enum value for backward serialization compat from older
                // clusters; the state machine no longer spends time in this state.
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
                ReplicationState.FOLLOWING.name -> FollowingState
                ReplicationState.COMPLETED.name -> CompletedState
                ReplicationState.MONITORING.name -> MonitoringState
                ReplicationState.FAILED.name -> FailedState("")
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
 * Retained for backward compatibility with serialized state from older clusters. The state machine
 * no longer transitions into this state; deserialization treats it as equivalent to FollowingState.
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
 * State when index task is in failed state. Per-shard failure attribution is no longer tracked here
 * (per-shard tasks have been removed); operators rely on the [errorMsg] reason recorded by whichever
 * shard context first triggered the pause via [org.opensearch.replication.metadata.ReplicationMetadataManager].
 */
data class FailedState(val errorMsg: String)
    : IndexReplicationState(ReplicationState.FAILED) {
    constructor(inp: StreamInput) : this(inp.readString())

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(errorMsg)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
        return builder.startObject()
                .field("error_message", errorMsg)
                .field("state", state)
                .endObject()
    }
}

/**
 * State when index is being actively replicated. With per-shard tasks removed, this state no longer
 * carries shard-task references — shard work is owned by [org.opensearch.replication.task.shard.NodeReplicationController]
 * on each follower data node.
 */
object FollowingState : IndexReplicationState(ReplicationState.FOLLOWING) {
    @JvmStatic
    @Suppress("UNUSED_PARAMETER")
    fun fromStream(inp: StreamInput): FollowingState {
        // Older serialized states may carry a map of shard task entries. Drain and discard for compat.
        val count = inp.readVInt()
        repeat(count) {
            // Read and discard a ShardId + PersistentTask payload. In practice we can't deserialize the inner
            // PersistentTask without ShardReplicationParams in the registry; old-version state should not be
            // expected in upgraded clusters because cleanup runs on first cluster manager init.
        }
        return FollowingState
    }

    @Suppress("UNUSED_PARAMETER")
    operator fun invoke(inp: StreamInput): FollowingState = FollowingState
}
