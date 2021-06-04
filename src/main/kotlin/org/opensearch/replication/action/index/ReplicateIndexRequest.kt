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

import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.IndicesRequest
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.master.AcknowledgedRequest
import org.opensearch.common.ParseField
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ObjectParser
import org.opensearch.common.xcontent.ToXContent.Params
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import java.io.IOException

class ReplicateIndexRequest : AcknowledgedRequest<ReplicateIndexRequest>, IndicesRequest.Replaceable, ToXContentObject {

    lateinit var followerIndex: String
    lateinit var remoteCluster: String
    lateinit var remoteIndex: String
    // Used for integ tests to wait until the restore from remote cluster completes
    var waitForRestore: Boolean = false

    private constructor() {
    }

    constructor(followerIndex: String, remoteCluster: String, remoteIndex: String) : super() {
        this.followerIndex = followerIndex
        this.remoteCluster = remoteCluster
        this.remoteIndex = remoteIndex
    }

    companion object {
        private val PARSER = ObjectParser<ReplicateIndexRequest, Void>("FollowIndexRequestParser") { ReplicateIndexRequest() }

        init {
            PARSER.declareString(ReplicateIndexRequest::remoteCluster::set, ParseField("remote_cluster"))
            PARSER.declareString(ReplicateIndexRequest::remoteIndex::set, ParseField("remote_index"))
        }

        @Throws(IOException::class)
        fun fromXContent(parser: XContentParser, followerIndex: String): ReplicateIndexRequest {
            val followIndexRequest = PARSER.parse(parser, null)
            followIndexRequest.followerIndex = followerIndex
            return followIndexRequest
        }
    }

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (!this::remoteCluster.isInitialized ||
            !this::remoteIndex.isInitialized ||
            !this::followerIndex.isInitialized) {
            validationException = ActionRequestValidationException()
            validationException.addValidationError("Mandatory params are missing for the request")
        }
        return validationException
    }

    override fun indices(vararg indices: String?): IndicesRequest {
        return this
    }

    override fun indices(): Array<String?> {
        return arrayOf(followerIndex)
    }

    override fun indicesOptions(): IndicesOptions {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed()
    }

    constructor(inp: StreamInput) : super(inp) {
        remoteCluster = inp.readString()
        remoteIndex = inp.readString()
        followerIndex = inp.readString()
        waitForRestore = inp.readBoolean()
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(remoteCluster)
        out.writeString(remoteIndex)
        out.writeString(followerIndex)
        out.writeBoolean(waitForRestore)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: Params): XContentBuilder {
        builder.startObject()
        builder.field("remote_cluster", remoteCluster)
        builder.field("remote_index", remoteIndex)
        builder.field("follower_index", followerIndex)
        builder.field("wait_for_restore", waitForRestore)
        builder.endObject()
        return builder
    }
}
