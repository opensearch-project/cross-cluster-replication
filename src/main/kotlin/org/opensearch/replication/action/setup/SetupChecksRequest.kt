package org.opensearch.replication.action.setup

import org.opensearch.replication.metadata.store.ReplicationContext
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.support.master.AcknowledgedRequest
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder

class SetupChecksRequest: AcknowledgedRequest<SetupChecksRequest>, ToXContentObject {
    val followerContext: ReplicationContext
    val leaderContext: ReplicationContext
    val connectionName: String

    constructor(followerContext: ReplicationContext,
                leaderContext: ReplicationContext,
                connectionName: String) {
        this.followerContext = followerContext
        this.leaderContext = leaderContext
        this.connectionName = connectionName
    }

    constructor(inp: StreamInput): super(inp) {
        this.followerContext = ReplicationContext(inp)
        this.leaderContext = ReplicationContext(inp)
        this.connectionName = inp.readString()
    }

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        followerContext.writeTo(out)
        leaderContext.writeTo(out)
        out.writeString(connectionName)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field("follower_context", this.followerContext)
        builder.field("leader_context", this.leaderContext)
        builder.field("connection_name", this.connectionName)
        return builder.endObject()
    }
}