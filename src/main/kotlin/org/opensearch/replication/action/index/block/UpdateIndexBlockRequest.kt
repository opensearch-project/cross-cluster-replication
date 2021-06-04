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

package org.opensearch.replication.action.index.block

import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.IndicesRequest
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.master.AcknowledgedRequest
import org.opensearch.common.ParseField
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ObjectParser
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder

enum class IndexBlockUpdateType {
    ADD_BLOCK, REMOVE_BLOCK
}

class UpdateIndexBlockRequest :  AcknowledgedRequest<UpdateIndexBlockRequest>, IndicesRequest, ToXContentObject {

    var indexName: String
    var updateType: IndexBlockUpdateType

    constructor(index: String, updateType: IndexBlockUpdateType): super() {
        this.indexName = index
        this.updateType = updateType
    }

    constructor(inp: StreamInput): super(inp) {
        indexName = inp.readString()
        updateType = inp.readEnum(IndexBlockUpdateType::class.java)
    }

    override fun validate(): ActionRequestValidationException? {
        /* No validation for now. Null checks are implicit as constructor doesn't
        allow nulls to be passed into the request.
         */
        return null;
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
        builder.field("updateType", updateType)
        builder.endObject()
        return builder
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(indexName)
        out.writeEnum(updateType)
    }
}