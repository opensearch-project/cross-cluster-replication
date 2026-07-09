/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.replication.action.autofollow

import org.opensearch.commons.authuser.User
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder

class AutoFollowClusterManagerNodeRequest: ClusterManagerNodeRequest<AutoFollowClusterManagerNodeRequest>, ToXContentObject {
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