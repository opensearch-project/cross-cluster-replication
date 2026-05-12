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

import org.opensearch.replication.metadata.store.KEY_SETTINGS
import org.opensearch.replication.util.ValidationUtil.validateName
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.IndicesRequest
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.clustermanager.AcknowledgedRequest
import org.opensearch.core.ParseField
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.common.settings.Settings
import org.opensearch.core.xcontent.ObjectParser
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContent.Params
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import java.io.IOException
import java.util.Collections
import java.util.function.BiConsumer
import java.util.function.BiFunction
import kotlin.collections.HashMap

class ReplicateIndexRequest : AcknowledgedRequest<ReplicateIndexRequest>, IndicesRequest.Replaceable, ToXContentObject {

    lateinit var followerIndex: String
    lateinit var leaderAlias: String
    lateinit var leaderIndex: String
    var useRoles: HashMap<String, String>? = null // roles to use - {leader_fgac_role: role1, follower_fgac_role: role2}
    // Used for integ tests to wait until the restore from leader cluster completes
    var waitForRestore: Boolean = false
    // Triggered from autofollow to skip permissions check based on user as this is already validated
    var isAutoFollowRequest: Boolean = false

    var settings :Settings = Settings.EMPTY

    private constructor() {
    }

    constructor(followerIndex: String, leaderAlias: String, leaderIndex: String, settings: Settings = Settings.EMPTY) : super() {
        this.followerIndex = followerIndex
        this.leaderAlias = leaderAlias
        this.leaderIndex = leaderIndex
        this.settings = settings
    }

    companion object {
        const val LEADER_CLUSTER_ROLE = "leader_cluster_role"
        const val FOLLOWER_CLUSTER_ROLE = "follower_cluster_role"
        private val INDEX_REQ_PARSER = ObjectParser<ReplicateIndexRequest, Void>("FollowIndexRequestParser") { ReplicateIndexRequest() }
        val FGAC_ROLES_PARSER = ObjectParser<HashMap<String, String>, Void>("UseRolesParser") { HashMap() }
        init {
            FGAC_ROLES_PARSER.declareStringOrNull({useRoles: HashMap<String, String>, role: String -> useRoles[LEADER_CLUSTER_ROLE] = role},
                    ParseField(LEADER_CLUSTER_ROLE))
            FGAC_ROLES_PARSER.declareStringOrNull({useRoles: HashMap<String, String>, role: String -> useRoles[FOLLOWER_CLUSTER_ROLE] = role},
                    ParseField(FOLLOWER_CLUSTER_ROLE))

            INDEX_REQ_PARSER.declareString(ReplicateIndexRequest::leaderAlias::set, ParseField("leader_alias"))
            INDEX_REQ_PARSER.declareString(ReplicateIndexRequest::leaderIndex::set, ParseField("leader_index"))
            INDEX_REQ_PARSER.declareObjectOrDefault(BiConsumer {reqParser: ReplicateIndexRequest, roles: HashMap<String, String> -> reqParser.useRoles = roles},
                    FGAC_ROLES_PARSER, null, ParseField("use_roles"))
            INDEX_REQ_PARSER.declareObjectOrDefault(
                { request: ReplicateIndexRequest, settings: Settings -> request.settings = settings},
                { p: XContentParser?, _: Void? -> Settings.fromXContent(p) },
                    null, ParseField(KEY_SETTINGS))
        }

        @Throws(IOException::class)
        fun fromXContent(parser: XContentParser, followerIndex: String): ReplicateIndexRequest {
            val followIndexRequest = INDEX_REQ_PARSER.parse(parser, null)
            followIndexRequest.followerIndex = followerIndex
            if(followIndexRequest.useRoles?.size == 0) {
                followIndexRequest.useRoles = null
            }

            return followIndexRequest
        }
    }

    override fun validate(): ActionRequestValidationException? {

        var validationException = ActionRequestValidationException()
        val missingFields: MutableList<String> = mutableListOf()
        if (!this::leaderAlias.isInitialized){
            missingFields.add("leader_alias")
        }
        if(!this::leaderIndex.isInitialized){
            missingFields.add("leader_index")
        }
        if (!this::followerIndex.isInitialized){
            missingFields.add("follower_index")
        }
        if(missingFields.isNotEmpty()){
            validationException.addValidationError("Mandatory params $missingFields are missing for the request")
            return validationException
        }

        validateName(leaderIndex, validationException)
        validateName(followerIndex, validationException)

        if(useRoles != null && (useRoles!!.size < 2 || useRoles!![LEADER_CLUSTER_ROLE] == null ||
                useRoles!![FOLLOWER_CLUSTER_ROLE] == null)) {
            validationException.addValidationError("Need roles for $LEADER_CLUSTER_ROLE and $FOLLOWER_CLUSTER_ROLE")
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
        leaderAlias = inp.readString()
        leaderIndex = inp.readString()
        followerIndex = inp.readString()

        var leaderClusterRole = inp.readOptionalString()
        var followerClusterRole = inp.readOptionalString()
        useRoles = HashMap()
        if(leaderClusterRole != null) useRoles!![LEADER_CLUSTER_ROLE] = leaderClusterRole
        if(followerClusterRole != null) useRoles!![FOLLOWER_CLUSTER_ROLE] = followerClusterRole

        waitForRestore = inp.readBoolean()
        isAutoFollowRequest = inp.readBoolean()
        settings = Settings.readSettingsFromStream(inp)

    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(leaderAlias)
        out.writeString(leaderIndex)
        out.writeString(followerIndex)
        out.writeOptionalString(useRoles?.get(LEADER_CLUSTER_ROLE))
        out.writeOptionalString(useRoles?.get(FOLLOWER_CLUSTER_ROLE))
        out.writeBoolean(waitForRestore)
        out.writeBoolean(isAutoFollowRequest)

        Settings.writeSettingsToStream(settings, out);
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: Params): XContentBuilder {
        builder.startObject()
        builder.field("leader_alias", leaderAlias)
        builder.field("leader_index", leaderIndex)
        builder.field("follower_index", followerIndex)
        if(useRoles != null && useRoles!!.size == 2) {
            builder.field("use_roles")
            builder.startObject()
            builder.field(LEADER_CLUSTER_ROLE, useRoles!![LEADER_CLUSTER_ROLE])
            builder.field(FOLLOWER_CLUSTER_ROLE, useRoles!![FOLLOWER_CLUSTER_ROLE])
            builder.endObject()
        }
        builder.field("wait_for_restore", waitForRestore)
        builder.field("is_autofollow_request", isAutoFollowRequest)

        builder.startObject(KEY_SETTINGS)
        settings.toXContent(builder, ToXContent.MapParams(Collections.singletonMap("flat_settings", "true")));
        builder.endObject()

        builder.endObject()

        return builder
    }
}
