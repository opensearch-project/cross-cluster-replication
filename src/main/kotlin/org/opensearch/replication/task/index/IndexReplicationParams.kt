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

package org.opensearch.replication.task.index

import org.opensearch.Version
import org.opensearch.common.ParseField
import org.opensearch.common.Strings
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ContextParser
import org.opensearch.common.xcontent.ObjectParser
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.index.Index
import org.opensearch.persistent.PersistentTaskParams
import java.io.IOException

class IndexReplicationParams : PersistentTaskParams {

    lateinit var remoteCluster: String
    lateinit var remoteIndex: Index
    lateinit var followerIndexName: String

    companion object {
        const val NAME = IndexReplicationExecutor.TASK_NAME

        private val PARSER = ObjectParser<IndexReplicationParams, Void>(NAME, true) { IndexReplicationParams() }
        init {
            PARSER.declareString(IndexReplicationParams::remoteCluster::set, ParseField("remote_cluster"))
            PARSER.declareObject(IndexReplicationParams::remoteIndex::set,
                    { parser: XContentParser, _ -> Index.fromXContent(parser) },
                    ParseField("remote_index"))
            PARSER.declareString(IndexReplicationParams::followerIndexName::set, ParseField("follower_index"))
        }

        @Throws(IOException::class)
        fun fromXContent(parser: XContentParser): IndexReplicationParams {
            return PARSER.parse(parser, null)
        }
    }

    constructor(remoteCluster: String, remoteIndex: Index, followerIndexName: String) {
        this.remoteCluster = remoteCluster
        this.remoteIndex = remoteIndex
        this.followerIndexName = followerIndexName
    }

    constructor(inp: StreamInput) : this(inp.readString(), Index(inp), inp.readString())

    private constructor() {
    }

    override fun getWriteableName(): String = NAME

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
        return builder.startObject()
            .field("remote_cluster", remoteCluster)
            .field("remote_index", remoteIndex)
            .field("follower_index", followerIndexName)
            .endObject()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(remoteCluster)
        remoteIndex.writeTo(out)
        out.writeString(followerIndexName)
    }

    override fun getMinimalSupportedVersion(): Version {
        return Version.V_1_0_0
    }

    override fun toString(): String {
        return Strings.toString(this)
    }
}
