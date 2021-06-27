package com.amazon.elasticsearch.replication.action.status


import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.support.DefaultShardOperationFailedException
import org.elasticsearch.action.support.broadcast.BroadcastResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent.Params
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import java.io.IOException

class ReplicationStatusResponse : BroadcastResponse, ToXContentObject {

    private val log = LogManager.getLogger(javaClass)
    lateinit var replicationShardResponse: MutableList<ReplicationStatusShardResponse>
    lateinit var status: String

    @Throws(IOException::class)
    constructor(inp: StreamInput) : super(inp) {
        inp.readList(::ReplicationStatusShardResponse)
        status = inp.readString()
    }

    constructor(
            totalShards: Int,
            successfulShards: Int,
            failedShards: Int,
            shardFailures: List<DefaultShardOperationFailedException>,
            shardResponses: List<ReplicationStatusShardResponse>
    ) : super(
            totalShards, successfulShards, failedShards, shardFailures
    ) {
        this.replicationShardResponse = shardResponses.toMutableList()
    }

    constructor(
            totalShards: Int,
            successfulShards: Int,
            failedShards: Int,
            shardFailures: List<DefaultShardOperationFailedException>,
            shardResponses: List<ReplicationStatusShardResponse>,
            status : String
    ) : super(
            totalShards, successfulShards, failedShards, shardFailures
    ) {
        this.replicationShardResponse = shardResponses.toMutableList()
        this.status = status
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: Params?): XContentBuilder {
        builder.startObject()
        builder.field("status",status)
        builder.field("replication_data",replicationShardResponse)
        builder.endObject()
        return builder
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeCollection(replicationShardResponse)
    }
}