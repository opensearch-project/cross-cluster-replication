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

package org.opensearch.replication.action.stop

import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.IndicesRequest
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.master.AcknowledgedRequest
import org.opensearch.core.ParseField
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.*

class StopIndexReplicationRequest : AcknowledgedRequest<StopIndexReplicationRequest>, IndicesRequest.Replaceable, ToXContentObject {

    lateinit var indexName: String

    constructor(indexName: String) {
        this.indexName = indexName
    }

    private constructor() {
    }

    constructor(inp: StreamInput): super(inp) {
        indexName = inp.readString()
    }

    companion object {
        private val PARSER = ObjectParser<StopIndexReplicationRequest, Void>("StopReplicationRequestParser") {
            StopIndexReplicationRequest()
        }

        fun fromXContent(parser: XContentParser, followerIndex: String): StopIndexReplicationRequest {
            val stopIndexReplicationRequest = PARSER.parse(parser, null)
            stopIndexReplicationRequest.indexName = followerIndex
            return stopIndexReplicationRequest
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
        builder.endObject()
        return builder
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(indexName)
    }

}