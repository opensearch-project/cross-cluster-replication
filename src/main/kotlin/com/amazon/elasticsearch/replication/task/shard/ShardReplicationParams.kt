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

import org.elasticsearch.Version
import org.elasticsearch.common.ParseField
import org.elasticsearch.common.Strings
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ObjectParser
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.persistent.PersistentTaskParams
import java.io.IOException
import org.elasticsearch.index.Index


class ShardReplicationParams : PersistentTaskParams {

    var leaderAlias: String
    var leaderShardId: ShardId
    var followerShardId: ShardId

    constructor(leaderAlias: String, leaderShardId: ShardId, followerShardId: ShardId) {
        this.leaderAlias = leaderAlias
        this.leaderShardId = leaderShardId
        this.followerShardId = followerShardId
    }

    constructor(inp : StreamInput) : this(inp.readString(), ShardId(inp), ShardId(inp))

    companion object {
        const val NAME = ShardReplicationExecutor.TASK_NAME

        private val PARSER = ObjectParser<Builder, Void>(ShardReplicationExecutor.TASK_NAME, true) { Builder() }
        init {
            PARSER.declareString(Builder::leaderAlias, ParseField("leader_alias"))
            // ShardId is converted to String - parsing from the same format to construct the params
            PARSER.declareString(Builder::leaderShardId, ParseField("leader_shard"))
            PARSER.declareString(Builder::leaderIndexUUID, ParseField("leader_index_uuid"))
            PARSER.declareString(Builder::followerShardId, ParseField("follower_shard"))
            PARSER.declareString(Builder::followerIndexUUID, ParseField("follower_index_uuid"))
        }

        @Throws(IOException::class)
        fun fromXContent(parser: XContentParser): ShardReplicationParams {
            return PARSER.parse(parser, null).build()
        }
    }

    override fun getWriteableName(): String {
        return NAME
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
        return builder.startObject()
            .field("leader_alias", leaderAlias)
            .field("leader_shard", leaderShardId)
            .field("leader_index_uuid", leaderShardId.index.uuid) // The XContent of ShardId doesn't serialize index uuid
            .field("follower_shard", followerShardId)
            .field("follower_index_uuid", followerShardId.index.uuid)
            .endObject()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(leaderAlias)
        leaderShardId.writeTo(out)
        followerShardId.writeTo(out)
    }

    override fun getMinimalSupportedVersion(): Version {
        return Version.V_7_1_0
    }

    override fun toString(): String {
        return Strings.toString(this)
    }

    class Builder {
        lateinit var leaderAlias: String
        lateinit var leaderShardId: String
        lateinit var leaderIndexUUID: String
        lateinit var followerShardId: String
        lateinit var followerIndexUUID: String

        fun leaderAlias(leaderAlias: String) {
            this.leaderAlias = leaderAlias
        }

        fun leaderShardId(leaderShardId: String) {
            this.leaderShardId = leaderShardId
        }

        fun leaderIndexUUID(leaderIndexUUID: String) {
            this.leaderIndexUUID = leaderIndexUUID
        }

        fun followerShardId(followerShardId: String) {
            this.followerShardId = followerShardId
        }

        fun followerIndexUUID(followerIndexUUID: String) {
            this.followerIndexUUID = followerIndexUUID
        }

        fun build(): ShardReplicationParams {
            val leaderShardIdObj = ShardId.fromString(leaderShardId)
            val followerShardIdObj = ShardId.fromString(followerShardId)
            return ShardReplicationParams(leaderAlias, ShardId(Index(leaderShardIdObj.indexName, leaderIndexUUID),
                    leaderShardIdObj.id), ShardId(Index(followerShardIdObj.indexName, followerIndexUUID),
                    followerShardIdObj.id))
        }
    }
}