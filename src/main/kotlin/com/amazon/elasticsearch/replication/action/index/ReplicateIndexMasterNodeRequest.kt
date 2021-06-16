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

package com.amazon.elasticsearch.replication.action.index

import com.amazon.opendistroforelasticsearch.commons.authuser.User
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.support.master.MasterNodeRequest
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder

class ReplicateIndexMasterNodeRequest:
        MasterNodeRequest<ReplicateIndexMasterNodeRequest>, ToXContentObject {

    var user: User?
    var replicateIndexReq: ReplicateIndexRequest

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    constructor(user: User?, replicateIndexReq: ReplicateIndexRequest): super() {
        this.user = user
        this.replicateIndexReq = replicateIndexReq
    }

    constructor(inp: StreamInput) : super(inp) {
        user = User(inp)
        replicateIndexReq = ReplicateIndexRequest(inp)
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        user?.writeTo(out)
        replicateIndexReq.writeTo(out)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        val responseBuilder =  builder.startObject()
                .field("user", user)
                .field("replication_request")
        return replicateIndexReq.toXContent(responseBuilder, params).endObject()
    }
}
