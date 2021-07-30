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

package com.amazon.elasticsearch.replication.action.pause

import com.amazon.elasticsearch.replication.metadata.ReplicationMetadataManager
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.IndicesRequest
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.master.AcknowledgedRequest
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ObjectParser
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser

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