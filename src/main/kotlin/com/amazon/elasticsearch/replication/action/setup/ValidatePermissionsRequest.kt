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
    var clusterRole: String? = null


    constructor(leaderAlias: String, leaderIndex: String, leaderClusterRole: String?) {
        this.cluster = leaderAlias
        this.index = leaderIndex
        this.clusterRole = leaderClusterRole
    }

    constructor(inp: StreamInput) : super(inp) {
        cluster = inp.readString()
        index = inp.readString()
        clusterRole = inp.readOptionalString()
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(cluster)
        out.writeString(index)
        out.writeOptionalString(clusterRole)
    }

    override fun validate(): ActionRequestValidationException? {
        // Request only accepts non-null string for leaderIndex
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
        builder.field("cluster_role", clusterRole)
        return builder.endObject()
    }
}
