package com.amazon.elasticsearch.replication.action.setup

import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput

class ValidatePermissionsResponse(val ack: Boolean): AcknowledgedResponse(ack) {
    constructor(inp: StreamInput) : this(inp.readBoolean())

    override fun writeTo(out: StreamOutput) {
        out.writeBoolean(ack)
    }
}
