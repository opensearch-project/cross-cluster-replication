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

package org.opensearch.replication.action.index.block

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
import java.util.function.Supplier

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