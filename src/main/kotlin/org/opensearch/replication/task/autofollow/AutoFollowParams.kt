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

    override fun getMinimalSupportedVersion(): Version = Version.V_2_0_0

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field("leader_cluster", leaderCluster)
            .field("pattern_name", patternName)
            .endObject()
    }
}