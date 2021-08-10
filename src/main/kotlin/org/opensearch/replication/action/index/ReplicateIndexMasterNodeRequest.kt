/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.replication.action.index

import org.opensearch.commons.authuser.User
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.support.master.MasterNodeRequest
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder

class ReplicateIndexMasterNodeRequest:
        MasterNodeRequest<ReplicateIndexMasterNodeRequest>, ToXContentObject {

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
