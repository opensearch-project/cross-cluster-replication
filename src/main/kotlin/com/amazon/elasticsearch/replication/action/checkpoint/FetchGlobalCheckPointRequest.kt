package com.amazon.elasticsearch.replication.action.checkpoint

import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.broadcast.BroadcastRequest
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.*

class FetchGlobalCheckPointRequest : BroadcastRequest<FetchGlobalCheckPointRequest> , ToXContentObject {

    companion object {
        private val log = LogManager.getLogger(FetchGlobalCheckPointRequest::class.java)
    }

    var indexName: String

    constructor(indexName: String) {
        this.indexName = indexName
    }

    constructor(inp: StreamInput): super(inp) {
        indexName = inp.readString()
    }

    override fun validate(): ActionRequestValidationException? {
        return null
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
