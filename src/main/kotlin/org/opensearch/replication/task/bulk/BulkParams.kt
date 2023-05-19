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

package org.opensearch.replication.task.bulk

import org.opensearch.Version
import org.opensearch.core.ParseField
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ObjectParser
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.persistent.PersistentTaskParams
import java.io.IOException

class BulkParams : PersistentTaskParams {

    lateinit var actionType: String
    lateinit var patternName: String

    companion object {
        const val NAME = BulkExecuter.TASK_NAME

        private val PARSER = ObjectParser<BulkParams, Void>(NAME, true) { BulkParams() }
        init {
            PARSER.declareString(BulkParams::actionType::set, ParseField("action_type"))
            PARSER.declareString(BulkParams::patternName::set, ParseField("pattern_name"))

        }

        @Throws(IOException::class)
        fun fromXContent(parser: XContentParser): BulkParams {
            return PARSER.parse(parser, null)
        }
    }

    private constructor() {
    }

    constructor(actionType: String, patternName: String, ) {
        this.actionType = actionType
        this.patternName = patternName

    }

    constructor(inp: StreamInput) : this(inp.readString(), inp.readString())

    override fun writeTo(out: StreamOutput) {
        out.writeString(actionType)
        out.writeString(patternName)

    }

    override fun getWriteableName() = NAME

    override fun getMinimalSupportedVersion() = Version.CURRENT

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field("action_type", actionType)
            .field("pattern_name", patternName)
            .endObject()
    }
}