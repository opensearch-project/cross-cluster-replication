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

package org.opensearch.replication.task.autofollow

import org.opensearch.Version
import org.opensearch.common.ParseField
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ObjectParser
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.persistent.PersistentTaskParams
import java.io.IOException

class AutoFollowParams : PersistentTaskParams {

    lateinit var remoteCluster: String
    lateinit var patternName: String

    companion object {
        const val NAME = AutoFollowExecutor.TASK_NAME

        private val PARSER = ObjectParser<AutoFollowParams, Void>(NAME, true) { AutoFollowParams() }
        init {
            PARSER.declareString(AutoFollowParams::remoteCluster::set, ParseField("remote_cluster"))
            PARSER.declareString(AutoFollowParams::patternName::set, ParseField("pattern_name"))
        }

        @Throws(IOException::class)
        fun fromXContent(parser: XContentParser): AutoFollowParams {
            return PARSER.parse(parser, null)
        }
    }

    private constructor() {
    }

    constructor(remoteCluster: String, patternName: String) {
        this.remoteCluster = remoteCluster
        this.patternName = patternName
    }

    constructor(inp: StreamInput) : this(inp.readString(), inp.readString())

    override fun writeTo(out: StreamOutput) {
        out.writeString(remoteCluster)
        out.writeString(patternName)
    }

    override fun getWriteableName() = NAME

    override fun getMinimalSupportedVersion() = Version.V_1_0_0

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field("remote_cluster", remoteCluster)
            .field("pattern_name", patternName)
            .endObject()
    }
}