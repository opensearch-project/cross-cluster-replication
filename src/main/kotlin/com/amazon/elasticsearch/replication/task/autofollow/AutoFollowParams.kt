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

package com.amazon.elasticsearch.replication.task.autofollow

import org.elasticsearch.Version
import org.elasticsearch.common.ParseField
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ObjectParser
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.persistent.PersistentTaskParams
import java.io.IOException

class AutoFollowParams : PersistentTaskParams {

    lateinit var leaderCluster: String
    lateinit var patternName: String

    companion object {
        const val NAME = AutoFollowExecutor.TASK_NAME

        private val PARSER = ObjectParser<AutoFollowParams, Void>(NAME, true) { AutoFollowParams() }
        init {
            PARSER.declareString(AutoFollowParams::leaderCluster::set, ParseField("leader_cluster"))
            PARSER.declareString(AutoFollowParams::patternName::set, ParseField("pattern_name"))
        }

        @Throws(IOException::class)
        fun fromXContent(parser: XContentParser): AutoFollowParams {
            return PARSER.parse(parser, null)
        }
    }

    private constructor() {
    }

    constructor(leaderCluster: String, patternName: String) {
        this.leaderCluster = leaderCluster
        this.patternName = patternName
    }

    constructor(inp: StreamInput) : this(inp.readString(), inp.readString())

    override fun writeTo(out: StreamOutput) {
        out.writeString(leaderCluster)
        out.writeString(patternName)
    }

    override fun getWriteableName() = NAME

    override fun getMinimalSupportedVersion() = Version.V_7_1_0

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field("leader_cluster", leaderCluster)
            .field("pattern_name", patternName)
            .endObject()
    }
}