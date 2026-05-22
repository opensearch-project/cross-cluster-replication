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

package org.opensearch.replication.task.shard

import org.opensearch.OpenSearchException
import org.opensearch.core.ParseField
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ObjectParser
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.persistent.PersistentTaskState
import org.opensearch.replication.task.ReplicationState
import java.io.IOException
import java.lang.IllegalArgumentException
import java.lang.IllegalStateException

/**
 * Compatibility-only stub for legacy per-shard persistent task state.
 *
 * The shard replication work is now in-memory and managed by [NodeReplicationController];
 * see `docs/remove-shard-tasks-design.md`. This sealed class is retained ONLY so that cluster
 * state persisted by older versions deserializes successfully on plugin start. The deserialized
 * entries are removed from cluster state by [CleanupShardTasksUpdateTask] shortly after cluster
 * manager initialization, and no executor is registered for [NAME] — meaning the persistent task
 * framework cannot allocate or schedule them in the new version.
 *
 * Field shape and wire format match the original deleted class exactly to preserve
 * backward-compatible deserialization from prior releases.
 */
sealed class ShardReplicationState : PersistentTaskState {

    var state: ReplicationState

    companion object {
        // Legacy task name - hard-coded because ShardReplicationExecutor was deleted in this PR.
        const val NAME = CleanupShardTasksUpdateTask.LEGACY_SHARD_TASK_NAME

        fun reader(inp: StreamInput): ShardReplicationState {
            val state = inp.readEnum(ReplicationState::class.java)!!
            return when (state) {
                ReplicationState.INIT -> throw IllegalStateException("INIT - Illegal state for shard replication task")
                ReplicationState.RESTORING -> throw IllegalStateException("RESTORING - Illegal state for shard replication task")
                ReplicationState.INIT_FOLLOW -> throw IllegalStateException("INIT_FOLLOW - Illegal state for shard replication task")
                ReplicationState.FOLLOWING -> FollowingShardState
                ReplicationState.FAILED -> FailedShardState(inp)
                ReplicationState.COMPLETED -> CompletedShardState
                else -> throw IllegalArgumentException("$state - Not a valid state for shard replication task")
            }
        }

        private val PARSER = ObjectParser<Builder, Void>(NAME, true) { Builder() }
        init {
            PARSER.declareString(Builder::setShardTaskState, ParseField("state"))
        }

        @Throws(IOException::class)
        fun fromXContent(parser: XContentParser): ShardReplicationState {
            return PARSER.parse(parser, null).build()
        }
    }

    constructor(state: ReplicationState) {
        this.state = state
    }

    override fun writeTo(out: StreamOutput) {
        out.writeEnum(state)
    }

    override fun getWriteableName(): String = NAME

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
        return builder.startObject()
            .field("state", state)
            .endObject()
    }

    class Builder {
        lateinit var state: String

        fun setShardTaskState(state: String) {
            this.state = state
        }

        fun build(): ShardReplicationState {
            // Issue details - https://github.com/opensearch-project/cross-cluster-replication/issues/223
            state = if (!this::state.isInitialized) ReplicationState.FOLLOWING.name else state
            return when (state) {
                ReplicationState.INIT.name -> throw IllegalArgumentException("INIT - Illegal state for shard replication task")
                ReplicationState.RESTORING.name -> throw IllegalArgumentException("RESTORING - Illegal state for shard replication task")
                ReplicationState.INIT_FOLLOW.name -> throw IllegalArgumentException("INIT_FOLLOW - Illegal state for shard replication task")
                ReplicationState.FOLLOWING.name -> FollowingShardState
                ReplicationState.COMPLETED.name -> CompletedShardState
                ReplicationState.FAILED.name -> FailedShardState(null)
                else -> throw IllegalArgumentException("$state - Not a valid state for shard replication task")
            }
        }
    }
}

/**
 * Renamed from `FollowingState` to `FollowingShardState` to avoid collision with
 * [org.opensearch.replication.task.index.FollowingState] (which has the same simple name in a
 * sibling package). The wire format and writeable name are unchanged — both old and new
 * deserialization paths produce the same on-the-wire bytes — so backward compatibility is
 * preserved.
 */
object FollowingShardState : ShardReplicationState(ReplicationState.FOLLOWING)

object CompletedShardState : ShardReplicationState(ReplicationState.COMPLETED)

/**
 * Renamed from `FailedState` to `FailedShardState` to avoid collision with
 * [org.opensearch.replication.task.index.FailedState].
 */
data class FailedShardState(val exception: OpenSearchException?)
    : ShardReplicationState(ReplicationState.FAILED) {
    constructor(inp: StreamInput) : this(inp.readException<OpenSearchException>())

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeException(exception)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
        builder.startObject()
        builder.field("exception")
        builder.startObject()
        exception?.toXContent(builder, params)
        builder.endObject()
        return builder.endObject()
    }
}
