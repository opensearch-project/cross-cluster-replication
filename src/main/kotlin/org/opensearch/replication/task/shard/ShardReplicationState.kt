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

import org.opensearch.replication.task.ReplicationState
import org.opensearch.OpenSearchException
import org.opensearch.common.ParseField
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ObjectParser
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.persistent.PersistentTaskState
import java.io.IOException
import java.lang.IllegalArgumentException
import java.lang.IllegalStateException

sealed class ShardReplicationState : PersistentTaskState {

    var state: ReplicationState
    companion object {
        const val NAME = ShardReplicationExecutor.TASK_NAME
        fun reader(inp : StreamInput): ShardReplicationState {
            val state = inp.readEnum(ReplicationState::class.java)!!
            return when(state) {
                ReplicationState.INIT -> throw IllegalStateException("INIT - Illegal state for shard replication task")
                ReplicationState.RESTORING -> throw IllegalStateException("RESTORING - Illegal state for shard replication task")
                ReplicationState.INIT_FOLLOW -> throw IllegalStateException("INIT_FOLLOW - Illegal state for shard replication task")
                ReplicationState.FOLLOWING -> FollowingState
                ReplicationState.FAILED -> FailedState(inp)
                ReplicationState.COMPLETED -> CompletedState
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

    override fun getWriteableName(): String {
        return NAME
    }

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
            state = if(!this::state.isInitialized) {
                ReplicationState.FOLLOWING.name
            } else {
                state
            }
            return when (state) {
                ReplicationState.INIT.name -> throw IllegalArgumentException("INIT - Illegal state for shard replication task")
                ReplicationState.RESTORING.name -> throw IllegalArgumentException("RESTORING - Illegal state for shard replication task")
                ReplicationState.INIT_FOLLOW.name -> throw IllegalArgumentException("INIT_FOLLOW - Illegal state for shard replication task")
                ReplicationState.FOLLOWING.name -> FollowingState
                ReplicationState.COMPLETED.name -> CompletedState
                ReplicationState.FAILED.name -> FailedState(null)
                else -> throw IllegalArgumentException("$state - Not a valid state for shard replication task")
            }
        }
    }
}


object FollowingState : ShardReplicationState(ReplicationState.FOLLOWING)
object CompletedState : ShardReplicationState(ReplicationState.COMPLETED)

/**
 * State when shard task is in failed state.
 */
data class FailedState(val exception: OpenSearchException?)
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

