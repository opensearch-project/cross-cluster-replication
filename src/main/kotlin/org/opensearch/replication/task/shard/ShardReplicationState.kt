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

package org.opensearch.replication.task.shard

import org.opensearch.replication.task.ReplicationState
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
                else -> throw IllegalArgumentException("$state - Not a valid state for shard replication task")
            }
        }
    }
}


object FollowingState : ShardReplicationState(ReplicationState.FOLLOWING)
object CompletedState : ShardReplicationState(ReplicationState.COMPLETED)

