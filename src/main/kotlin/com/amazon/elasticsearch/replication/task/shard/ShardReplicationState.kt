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

package com.amazon.elasticsearch.replication.task.shard

import com.amazon.elasticsearch.replication.task.ReplicationState
import com.amazon.elasticsearch.replication.task.index.IndexReplicationState
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.common.ParseField
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ObjectParser
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.persistent.PersistentTaskState
import org.elasticsearch.persistent.PersistentTasksCustomMetadata
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
            PARSER.declareString(Builder::state, ParseField("state"))
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

        fun state(state: String) {
            this.state = state
        }
        fun build(): ShardReplicationState {
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
data class FailedState(val exception: ElasticsearchException?)
    : ShardReplicationState(ReplicationState.FAILED) {
    constructor(inp: StreamInput) : this(inp.readException<ElasticsearchException>())

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

