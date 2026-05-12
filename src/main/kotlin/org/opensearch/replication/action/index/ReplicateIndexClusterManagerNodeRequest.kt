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

package org.opensearch.replication.action.index

import org.opensearch.commons.authuser.User
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder

class ReplicateIndexClusterManagerNodeRequest:
    ClusterManagerNodeRequest<ReplicateIndexClusterManagerNodeRequest>, ToXContentObject {

    var user: User? = null
    var replicateIndexReq: ReplicateIndexRequest
    var withSecurityContext: Boolean = false

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    constructor(user: User?, replicateIndexReq: ReplicateIndexRequest): super() {
        this.user = user
        this.replicateIndexReq = replicateIndexReq
        if (this.user != null) {
            this.withSecurityContext = true
        }
    }

    constructor(inp: StreamInput) : super(inp) {
        this.withSecurityContext = inp.readBoolean()
        if(withSecurityContext) {
            user = User(inp)
        }
        replicateIndexReq = ReplicateIndexRequest(inp)
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeBoolean(withSecurityContext)
        if(this.withSecurityContext) {
            user?.writeTo(out)
        }
        replicateIndexReq.writeTo(out)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        val responseBuilder =  builder.startObject()
                .field("user", user)
                .field("replication_request")
        replicateIndexReq.toXContent(responseBuilder, params).endObject()
        return builder.field("with_security_context", withSecurityContext)
    }
}
