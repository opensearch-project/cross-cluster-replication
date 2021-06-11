package com.amazon.elasticsearch.replication.action.autofollow

import com.amazon.opendistroforelasticsearch.commons.authuser.User
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.support.master.MasterNodeRequest
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder

class AutoFollowMasterNodeRequest: MasterNodeRequest<AutoFollowMasterNodeRequest>, ToXContentObject {
    var user: User? = null
    var autofollowReq: UpdateAutoFollowPatternRequest
    var withSecurityContext: Boolean = false

    constructor(user: User?, autofollowReq: UpdateAutoFollowPatternRequest): super() {
        this.user = user
        if(user != null) {
            this.withSecurityContext = true
        }
        this.autofollowReq = autofollowReq
    }

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    constructor(inp: StreamInput): super(inp) {
        this.withSecurityContext = inp.readBoolean()
        if(withSecurityContext) {
            this.user = User(inp)
        }
        autofollowReq = UpdateAutoFollowPatternRequest(inp)
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeBoolean(withSecurityContext)
        if(this.withSecurityContext) {
            user?.writeTo(out)
        }
        autofollowReq.writeTo(out)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field("user", user)
        builder.field("auto_follow_req")
        autofollowReq.toXContent(builder, params)
        builder.field("with_security_context", withSecurityContext)
        return builder.endObject()
    }

}