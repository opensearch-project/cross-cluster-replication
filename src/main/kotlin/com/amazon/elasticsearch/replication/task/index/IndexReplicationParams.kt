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

package com.amazon.elasticsearch.replication.task.index

import org.elasticsearch.Version
import org.elasticsearch.common.ParseField
import org.elasticsearch.common.Strings
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ObjectParser
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.index.Index
import org.elasticsearch.persistent.PersistentTaskParams
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

    override fun getMinimalSupportedVersion(): Version {
        return Version.V_7_1_0
    }

    override fun toString(): String {
        return Strings.toString(this)
    }
}
