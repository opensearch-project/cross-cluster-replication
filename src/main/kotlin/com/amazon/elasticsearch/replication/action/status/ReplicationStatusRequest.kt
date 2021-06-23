package com.amazon.elasticsearch.replication.action.status

import org.elasticsearch.action.support.broadcast.BroadcastRequest
import org.elasticsearch.common.io.stream.StreamInput
import java.io.IOException

class ReplicationStatusRequest : BroadcastRequest<ReplicationStatusRequest> {
    @Suppress("SpreadOperator")
    constructor(vararg indices: String) : super(*indices)

    @Throws(IOException::class)
    constructor(inp: StreamInput) : super(inp)
}