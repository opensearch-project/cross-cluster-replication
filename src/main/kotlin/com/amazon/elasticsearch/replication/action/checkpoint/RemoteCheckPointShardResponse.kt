package com.amazon.elasticsearch.replication.action.checkpoint



import org.elasticsearch.action.support.broadcast.BroadcastShardResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.index.shard.ShardId
import java.io.IOException

class RemoteCheckPointShardResponse : BroadcastShardResponse {
    var shardGlobalCheckPoint: Long

    constructor(si: StreamInput) : super(si) {
        shardGlobalCheckPoint = si.readLong()
    }

    constructor(shardId: ShardId, shardGlobalCheckPoint: Long) : super(shardId) {
        this.shardGlobalCheckPoint = shardGlobalCheckPoint
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeLong(shardGlobalCheckPoint)
    }
}