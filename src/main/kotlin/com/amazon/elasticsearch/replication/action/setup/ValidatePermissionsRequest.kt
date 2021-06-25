package com.amazon.elasticsearch.replication.action.setup

import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.IndicesRequest
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.master.AcknowledgedRequest
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder

class ValidatePermissionsRequest: AcknowledgedRequest<ValidatePermissionsRequest>, IndicesRequest.Replaceable, ToXContentObject {
    val cluster: String
    val index: String
    var fgacRole: String? = null


    constructor(remoteCluster: String, remoteIndex: String, leaderFgacRole: String?) {
        this.cluster = remoteCluster
        this.index = remoteIndex
        this.fgacRole = leaderFgacRole
    }

    constructor(inp: StreamInput) : super(inp) {
        cluster = inp.readString()
        index = inp.readString()
        fgacRole = inp.readOptionalString()
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(cluster)
        out.writeString(index)
        out.writeOptionalString(fgacRole)
    }

    override fun validate(): ActionRequestValidationException? {
        // Request only accepts non-null string for remoteIndex
        return null
    }

    override fun indices(vararg indices: String?): IndicesRequest {
        return this
    }

    override fun indices(): Array<String> {
        return arrayOf(index)
    }

    override fun indicesOptions(): IndicesOptions {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed()
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field("cluster", cluster)
        builder.field("resource", index)
        builder.field("fgac_role", fgacRole)
        return builder.endObject()
    }
}
