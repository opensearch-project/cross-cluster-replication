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
import org.opensearch.core.ParseField
import org.opensearch.core.common.Strings
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.index.Index
import org.opensearch.core.index.shard.ShardId
import org.opensearch.core.xcontent.ObjectParser
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentType
import org.opensearch.persistent.PersistentTaskParams
import java.io.IOException

/**
 * Compatibility-only stub for legacy per-shard persistent task params.
 *
 * The shard replication work is now in-memory and managed by [NodeReplicationController];
 * see `docs/remove-shard-tasks-design.md`. This class is retained ONLY so that cluster state
 * persisted by older versions deserializes successfully on plugin start. The deserialized
 * entries are removed from cluster state by [CleanupShardTasksUpdateTask] shortly after
 * cluster manager initialization, and no executor is registered for [NAME] — meaning the
 * persistent task framework cannot allocate or schedule them in the new version.
 *
 * Field shape and wire format match the original deleted class exactly to preserve
 * backward-compatible deserialization from prior releases.
 */
class ShardReplicationParams : PersistentTaskParams {

    var leaderAlias: String
    var leaderShardId: ShardId
    var followerShardId: ShardId

    constructor(leaderAlias: String, leaderShardId: ShardId, followerShardId: ShardId) {
        this.leaderAlias = leaderAlias
        this.leaderShardId = leaderShardId
        this.followerShardId = followerShardId
    }

    constructor(inp: StreamInput) : this(inp.readString(), ShardId(inp), ShardId(inp))

    companion object {
        // Legacy task name - hard-coded because ShardReplicationExecutor was deleted in this PR.
        const val NAME = CleanupShardTasksUpdateTask.LEGACY_SHARD_TASK_NAME

        private val PARSER = ObjectParser<Builder, Void>(NAME, true) { Builder() }
        init {
            PARSER.declareString(Builder::leaderAlias, ParseField("leader_alias"))
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

    override fun getWriteableName(): String = NAME

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
        return builder.startObject()
            .field("leader_alias", leaderAlias)
            .field("leader_shard", leaderShardId)
            .field("leader_index_uuid", leaderShardId.index.uuid)
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

    override fun toString(): String = Strings.toString(XContentType.JSON, this)

    class Builder {
        lateinit var leaderAlias: String
        lateinit var leaderShardId: String
        lateinit var leaderIndexUUID: String
        lateinit var followerShardId: String
        lateinit var followerIndexUUID: String

        fun leaderAlias(leaderAlias: String) { this.leaderAlias = leaderAlias }
        fun leaderShardId(leaderShardId: String) { this.leaderShardId = leaderShardId }
        fun leaderIndexUUID(leaderIndexUUID: String) { this.leaderIndexUUID = leaderIndexUUID }
        fun followerShardId(followerShardId: String) { this.followerShardId = followerShardId }
        fun followerIndexUUID(followerIndexUUID: String) { this.followerIndexUUID = followerIndexUUID }

        fun build(): ShardReplicationParams {
            val leaderShardIdObj = ShardId.fromString(leaderShardId)
            val followerShardIdObj = ShardId.fromString(followerShardId)
            return ShardReplicationParams(
                leaderAlias,
                ShardId(Index(leaderShardIdObj.indexName, leaderIndexUUID), leaderShardIdObj.id),
                ShardId(Index(followerShardIdObj.indexName, followerIndexUUID), followerShardIdObj.id),
            )
        }
    }
}
