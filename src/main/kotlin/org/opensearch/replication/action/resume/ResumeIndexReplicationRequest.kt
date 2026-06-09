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

package org.opensearch.replication.action.resume

import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.IndicesRequest
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.clustermanager.AcknowledgedRequest
import org.opensearch.core.ParseField
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.*

class ResumeIndexReplicationRequest : AcknowledgedRequest<ResumeIndexReplicationRequest>, IndicesRequest.Replaceable, ToXContentObject {

    lateinit var indexName: String
    @JvmField
    var forceResume: Boolean = false

    constructor(indexName: String, forceResume: Boolean = false) {
        this.indexName = indexName
        this.forceResume = forceResume
    }

    private constructor() {
    }

    constructor(inp: StreamInput): super(inp) {
        indexName = inp.readString()
        forceResume = inp.readBoolean()
    }

    companion object {
        private const val FORCE_RESUME_FIELD = "force_resume"

        private val PARSER = ObjectParser<ResumeIndexReplicationRequest, Void>("ResumeReplicationRequestParser") {
            ResumeIndexReplicationRequest()
        }

        init {
            PARSER.declareBoolean({ req, value -> req.forceResume = value }, ParseField(FORCE_RESUME_FIELD))
        }

        fun fromXContent(parser: XContentParser, followerIndex: String): ResumeIndexReplicationRequest {
            val resumeRequest = PARSER.parse(parser, null)
            resumeRequest.indexName = followerIndex
            return resumeRequest
        }
    }

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    override fun indices(vararg indices: String?): IndicesRequest {
        return this
    }

    override fun indices(): Array<String> {
        return arrayOf(indexName)
    }

    override fun indicesOptions(): IndicesOptions {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed()
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field("indexName", indexName)
        builder.field("force_resume", forceResume)
        builder.endObject()
        return builder
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(indexName)
        out.writeBoolean(forceResume)
    }

}