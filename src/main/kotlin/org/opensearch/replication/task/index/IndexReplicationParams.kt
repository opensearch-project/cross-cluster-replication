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

import org.opensearch.Version
import org.opensearch.common.ParseField
import org.opensearch.common.Strings
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ObjectParser
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.index.Index
import org.opensearch.persistent.PersistentTaskParams
import java.io.IOException

class IndexReplicationParams : PersistentTaskParams {

    lateinit var leaderAlias: String
    lateinit var leaderIndex: Index
    lateinit var followerIndexName: String

    companion object {
        const val NAME = IndexReplicationExecutor.TASK_NAME

        private val PARSER = ObjectParser<IndexReplicationParams, Void>(NAME, true) { IndexReplicationParams() }
        init {
            PARSER.declareString(IndexReplicationParams::leaderAlias::set, ParseField("leader_alias"))
            PARSER.declareObject(IndexReplicationParams::leaderIndex::set,
                    { parser: XContentParser, _ -> Index.fromXContent(parser) },
                    ParseField("leader_index"))
            PARSER.declareString(IndexReplicationParams::followerIndexName::set, ParseField("follower_index"))
        }

        @Throws(IOException::class)
        fun fromXContent(parser: XContentParser): IndexReplicationParams {
            return PARSER.parse(parser, null)
        }
    }

    constructor(leaderAlias: String, leaderIndex: Index, followerIndexName: String) {
        this.leaderAlias = leaderAlias
        this.leaderIndex = leaderIndex
        this.followerIndexName = followerIndexName
    }

    constructor(inp: StreamInput) : this(inp.readString(), Index(inp), inp.readString())

    private constructor() {
    }

    override fun getWriteableName(): String = NAME

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
        return builder.startObject()
            .field("leader_alias", leaderAlias)
            .field("leader_index", leaderIndex)
            .field("follower_index", followerIndexName)
            .endObject()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(leaderAlias)
        leaderIndex.writeTo(out)
        out.writeString(followerIndexName)
    }

    override fun getMinimalSupportedVersion(): Version = Version.V_2_0_0

    override fun toString(): String {
        return Strings.toString(this)
    }
}
