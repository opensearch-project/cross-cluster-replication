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

import org.opensearch.Version
import org.opensearch.common.ParseField
import org.opensearch.common.Strings
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ObjectParser
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.index.shard.ShardId
import org.opensearch.persistent.PersistentTaskParams
import java.io.IOException
import org.opensearch.index.Index


class ShardReplicationParams : PersistentTaskParams {

    var remoteCluster: String
    var remoteShardId: ShardId
    var followerShardId: ShardId

    constructor(remoteCluster: String, remoteShardId: ShardId, followerShardId: ShardId) {
        this.remoteCluster = remoteCluster
        this.remoteShardId = remoteShardId
        this.followerShardId = followerShardId
    }

    constructor(inp : StreamInput) : this(inp.readString(), ShardId(inp), ShardId(inp))

    companion object {
        const val NAME = ShardReplicationExecutor.TASK_NAME

        private val PARSER = ObjectParser<Builder, Void>(ShardReplicationExecutor.TASK_NAME, true) { Builder() }
        init {
            PARSER.declareString(Builder::remoteCluster, ParseField("remote_cluster"))
            // ShardId is converted to String - parsing from the same format to construct the params
            PARSER.declareString(Builder::remoteShardId, ParseField("remote_shard"))
            PARSER.declareString(Builder::remoteIndexUUID, ParseField("remote_index_uuid"))
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
            .field("remote_cluster", remoteCluster)
            .field("remote_shard", remoteShardId)
            .field("remote_index_uuid", remoteShardId.index.uuid) // The XContent of ShardId doesn't serialize index uuid
            .field("follower_shard", followerShardId)
            .field("follower_index_uuid", followerShardId.index.uuid)
            .endObject()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(remoteCluster)
        remoteShardId.writeTo(out)
        followerShardId.writeTo(out)
    }

    override fun getMinimalSupportedVersion(): Version {
        return Version.V_1_0_0
    }

    override fun toString(): String {
        return Strings.toString(this)
    }

    class Builder {
        lateinit var remoteCluster: String
        lateinit var remoteShardId: String
        lateinit var remoteIndexUUID: String
        lateinit var followerShardId: String
        lateinit var followerIndexUUID: String

        fun remoteCluster(remoteCluster: String) {
            this.remoteCluster = remoteCluster
        }

        fun remoteShardId(remoteShardId: String) {
            this.remoteShardId = remoteShardId
        }

        fun remoteIndexUUID(remoteIndexUUID: String) {
            this.remoteIndexUUID = remoteIndexUUID
        }

        fun followerShardId(followerShardId: String) {
            this.followerShardId = followerShardId
        }

        fun followerIndexUUID(followerIndexUUID: String) {
            this.followerIndexUUID = followerIndexUUID
        }

        fun build(): ShardReplicationParams {
            val remoteShardIdObj = ShardId.fromString(remoteShardId)
            val followerShardIdObj = ShardId.fromString(followerShardId)
            return ShardReplicationParams(remoteCluster, ShardId(Index(remoteShardIdObj.indexName, remoteIndexUUID),
                    remoteShardIdObj.id), ShardId(Index(followerShardIdObj.indexName, followerIndexUUID),
                    followerShardIdObj.id))
        }
    }
}