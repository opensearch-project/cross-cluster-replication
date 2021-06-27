package com.amazon.elasticsearch.replication.action.checkpoint



import org.elasticsearch.action.support.DefaultShardOperationFailedException
import org.elasticsearch.action.support.broadcast.BroadcastResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent.Params
import org.elasticsearch.common.xcontent.XContentBuilder
import java.io.IOException

class RemoteGlobalCheckPointResponse : BroadcastResponse {

    lateinit var shardResponses: MutableList<RemoteCheckPointShardResponse>


    @Throws(IOException::class)
    constructor(inp: StreamInput) : super(inp) {
        shardResponses = inp.readList(::RemoteCheckPointShardResponse)
    }

    constructor(
            totalShards: Int,
            successfulShards: Int,
            failedShards: Int,
            shardFailures: List<DefaultShardOperationFailedException>,
            shardResponses: List<RemoteCheckPointShardResponse>
    ) : super(
            totalShards, successfulShards, failedShards, shardFailures
    ) {
        this.shardResponses = shardResponses.toMutableList()
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: Params?): XContentBuilder? {
        builder.startObject()
        builder.field("shard_responses",shardResponses)
        builder.endObject()
        return builder
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeCollection(shardResponses)
    }
}