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

import com.amazon.elasticsearch.replication.metadata.state.FollowIndexName
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.IndicesRequest
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.master.AcknowledgedRequest
import org.elasticsearch.common.ParseField
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ObjectParser
import org.elasticsearch.common.xcontent.ToXContent.Params
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import java.io.IOException
import java.util.function.BiConsumer

class ReplicateIndexRequest : AcknowledgedRequest<ReplicateIndexRequest>, IndicesRequest.Replaceable, ToXContentObject {

    lateinit var followerIndex: String
    lateinit var remoteCluster: String
    lateinit var remoteIndex: String
    var assumeRoles: HashMap<String, String>? = null // roles to assume - {leader_fgac_role: role1, follower_fgac_role: role2}
    // Used for integ tests to wait until the restore from remote cluster completes
    var waitForRestore: Boolean = false
    // Triggered from autofollow to skip permissions check based on user as this is already validated
    var isAutoFollowRequest: Boolean = false

    private constructor() {
    }

    constructor(followerIndex: String, remoteCluster: String, remoteIndex: String) : super() {
        this.followerIndex = followerIndex
        this.remoteCluster = remoteCluster
        this.remoteIndex = remoteIndex
    }

    companion object {
        const val LEADER_FGAC_ROLE = "remote_cluster_role"
        const val FOLLOWER_FGAC_ROLE = "local_cluster_role"
        private val INDEX_REQ_PARSER = ObjectParser<ReplicateIndexRequest, Void>("FollowIndexRequestParser") { ReplicateIndexRequest() }
        val FGAC_ROLES_PARSER = ObjectParser<HashMap<String, String>, Void>("AssumeRolesParser") { HashMap() }
        init {
            FGAC_ROLES_PARSER.declareStringOrNull({assumeRoles: HashMap<String, String>, role: String -> assumeRoles[LEADER_FGAC_ROLE] = role},
                    ParseField(LEADER_FGAC_ROLE))
            FGAC_ROLES_PARSER.declareStringOrNull({assumeRoles: HashMap<String, String>, role: String -> assumeRoles[FOLLOWER_FGAC_ROLE] = role},
                    ParseField(FOLLOWER_FGAC_ROLE))

            INDEX_REQ_PARSER.declareString(ReplicateIndexRequest::remoteCluster::set, ParseField("remote_cluster"))
            INDEX_REQ_PARSER.declareString(ReplicateIndexRequest::remoteIndex::set, ParseField("remote_index"))
            INDEX_REQ_PARSER.declareObjectOrDefault(BiConsumer {reqParser: ReplicateIndexRequest, roles: HashMap<String, String> -> reqParser.assumeRoles = roles},
                    FGAC_ROLES_PARSER, null, ParseField("assume_roles"))
        }

        @Throws(IOException::class)
        fun fromXContent(parser: XContentParser, followerIndex: String): ReplicateIndexRequest {
            val followIndexRequest = INDEX_REQ_PARSER.parse(parser, null)
            followIndexRequest.followerIndex = followerIndex
            if(followIndexRequest.assumeRoles?.size == 0) {
                followIndexRequest.assumeRoles = null
            }
            return followIndexRequest
        }
    }

    override fun validate(): ActionRequestValidationException? {
        var validationException = ActionRequestValidationException()
        if (!this::remoteCluster.isInitialized ||
            !this::remoteIndex.isInitialized ||
            !this::followerIndex.isInitialized) {
            validationException.addValidationError("Mandatory params are missing for the request")
        }

        if(assumeRoles != null && (assumeRoles!!.size < 2 || assumeRoles!![LEADER_FGAC_ROLE] == null ||
                assumeRoles!![FOLLOWER_FGAC_ROLE] == null)) {
            validationException.addValidationError("Need roles for $LEADER_FGAC_ROLE and $FOLLOWER_FGAC_ROLE")
        }
        return if(validationException.validationErrors().isEmpty()) return null else validationException
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

        var leaderFgacRole = inp.readOptionalString()
        var followerFgacRole = inp.readOptionalString()
        assumeRoles = HashMap()
        if(leaderFgacRole != null) assumeRoles!![LEADER_FGAC_ROLE] = leaderFgacRole
        if(followerFgacRole != null) assumeRoles!![FOLLOWER_FGAC_ROLE] = followerFgacRole

        waitForRestore = inp.readBoolean()
        isAutoFollowRequest = inp.readBoolean()
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(remoteCluster)
        out.writeString(remoteIndex)
        out.writeString(followerIndex)
        out.writeOptionalString(assumeRoles?.get(LEADER_FGAC_ROLE))
        out.writeOptionalString(assumeRoles?.get(FOLLOWER_FGAC_ROLE))
        out.writeBoolean(waitForRestore)
        out.writeBoolean(isAutoFollowRequest)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: Params): XContentBuilder {
        builder.startObject()
        builder.field("remote_cluster", remoteCluster)
        builder.field("remote_index", remoteIndex)
        builder.field("follower_index", followerIndex)
        if(assumeRoles != null && assumeRoles!!.size == 2) {
            builder.field("assume_roles")
            builder.startObject()
            builder.field(LEADER_FGAC_ROLE, assumeRoles!![LEADER_FGAC_ROLE])
            builder.field(FOLLOWER_FGAC_ROLE, assumeRoles!![FOLLOWER_FGAC_ROLE])
            builder.endObject()
        }
        builder.field("wait_for_restore", waitForRestore)
        builder.field("is_autofollow_request", isAutoFollowRequest)
        builder.endObject()
        return builder
    }
}
