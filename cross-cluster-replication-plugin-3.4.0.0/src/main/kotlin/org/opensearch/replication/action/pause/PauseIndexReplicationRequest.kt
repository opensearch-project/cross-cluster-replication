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

package org.opensearch.replication.action.pause

import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.IndicesRequest
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.clustermanager.AcknowledgedRequest
import org.opensearch.core.ParseField
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ObjectParser
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser

class PauseIndexReplicationRequest : AcknowledgedRequest<PauseIndexReplicationRequest>, IndicesRequest.Replaceable, ToXContentObject {

    lateinit var indexName: String
    var reason = ReplicationMetadataManager.CUSTOMER_INITIATED_ACTION

    constructor(indexName: String, reason: String) {
        this.indexName = indexName
        this.reason = reason
    }

    private constructor() {
    }

    constructor(inp: StreamInput): super(inp) {
        indexName = inp.readString()
        reason = inp.readString()
    }

    companion object {
        private val PARSER = ObjectParser<PauseIndexReplicationRequest, Void>("PauseReplicationRequestParser") {
            PauseIndexReplicationRequest()
        }

        init {
            PARSER.declareString(PauseIndexReplicationRequest::reason::set, ParseField("reason"))
        }

        fun fromXContent(parser: XContentParser, followerIndex: String): PauseIndexReplicationRequest {
            val PauseIndexReplicationRequest = PARSER.parse(parser, null)
            PauseIndexReplicationRequest.indexName = followerIndex
            return PauseIndexReplicationRequest
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
        out.writeString(reason)
    }

}
