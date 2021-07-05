package com.amazon.elasticsearch.replication.action.status


import org.elasticsearch.action.support.DefaultShardOperationFailedException
import org.elasticsearch.action.support.broadcast.BroadcastResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent.Params
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import java.io.IOException

class ReplicationStatusResponse : BroadcastResponse, ToXContentObject {

    lateinit var shardInfoResponse: MutableList<ShardInfoResponse>
    lateinit var status: String
    lateinit var connectionAlias: String
    lateinit var leaderIndexName: String
    lateinit var followerIndexName: String

    @Throws(IOException::class)
    constructor(inp: StreamInput) : super(inp) {
        shardInfoResponse = inp.readList(::ShardInfoResponse)
    }

    constructor(
            totalShards: Int,
            successfulShards: Int,
            failedShards: Int,
            shardFailures: List<DefaultShardOperationFailedException>,
            shardInfoRespons: List<ShardInfoResponse>
    ) : super(
            totalShards, successfulShards, failedShards, shardFailures
    ) {
        this.shardInfoResponse = shardInfoRespons.toMutableList()
    }


    constructor(
            status : String
    ) {
        this.status = status
    }

    constructor(
            totalShards: Int,
            successfulShards: Int,
            failedShards: Int,
            shardFailures: List<DefaultShardOperationFailedException>,
            shardInfoResponse: List<ShardInfoResponse>,
            status : String
    ) : super(
            totalShards, successfulShards, failedShards, shardFailures
    ) {
        this.shardInfoResponse = shardInfoResponse.toMutableList()
        this.status = status
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: Params?): XContentBuilder {
        builder.startObject()
        if (::status.isInitialized)
            builder.field("status",status)
        if (::connectionAlias.isInitialized)
            builder.field("connection_alias",connectionAlias)
        if (::leaderIndexName.isInitialized)
            builder.field("leader_index",leaderIndexName)
        if (::followerIndexName.isInitialized)
            builder.field("follower_index",followerIndexName)
        if (::shardInfoResponse.isInitialized)
            builder.field("replication_data",shardInfoResponse)
        builder.endObject()
        return builder
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        if (::shardInfoResponse.isInitialized)
            out.writeList(shardInfoResponse)
        if (::status.isInitialized)
            out.writeString(status)
        if (::connectionAlias.isInitialized)
            out.writeString(connectionAlias)
        if (::leaderIndexName.isInitialized)
            out.writeString(leaderIndexName)
        if (::followerIndexName.isInitialized)
            out.writeString(followerIndexName)
    }

    override fun toString(): String {
        return "ReplicationStatusResponse(shardInfoResponse=$shardInfoResponse, status='$status', connectionAlias='$connectionAlias', leaderIndexName='$leaderIndexName', followerIndexName='$followerIndexName')"
    }


}
