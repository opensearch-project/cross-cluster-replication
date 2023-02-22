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

    override fun getMinimalSupportedVersion(): Version = Version.V_2_0_0

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