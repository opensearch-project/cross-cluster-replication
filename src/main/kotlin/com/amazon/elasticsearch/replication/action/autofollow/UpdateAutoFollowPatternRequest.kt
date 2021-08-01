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

package com.amazon.elasticsearch.replication.action.autofollow

import com.amazon.elasticsearch.replication.action.index.ReplicateIndexRequest
import com.amazon.elasticsearch.replication.metadata.store.KEY_SETTINGS
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.support.master.AcknowledgedRequest
import org.elasticsearch.common.ParseField
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.ObjectParser
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import java.util.Collections
import java.util.function.BiConsumer
import java.util.function.BiFunction
import kotlin.collections.HashMap

class UpdateAutoFollowPatternRequest: AcknowledgedRequest<UpdateAutoFollowPatternRequest>, ToXContentObject {

    lateinit var connection: String
    lateinit var patternName: String
    var pattern: String? = null
    var assumeRoles: HashMap<String, String>? = null // roles to assume - {leader_fgac_role: role1, follower_fgac_role: role2}
    var settings : Settings = Settings.EMPTY

    enum class Action {
        ADD, REMOVE
    }
    lateinit var action : Action

    private constructor()

    companion object {
        private val AUTOFOLLOW_REQ_PARSER = ObjectParser<UpdateAutoFollowPatternRequest, Void>("AutoFollowRequestParser") { UpdateAutoFollowPatternRequest() }
        init {
            AUTOFOLLOW_REQ_PARSER.declareString(UpdateAutoFollowPatternRequest::connection::set, ParseField("leader_alias"))
            AUTOFOLLOW_REQ_PARSER.declareString(UpdateAutoFollowPatternRequest::patternName::set, ParseField("name"))
            AUTOFOLLOW_REQ_PARSER.declareString(UpdateAutoFollowPatternRequest::pattern::set, ParseField("pattern"))

            AUTOFOLLOW_REQ_PARSER.declareObjectOrDefault(BiConsumer { reqParser: UpdateAutoFollowPatternRequest,
                                                                      roles: HashMap<String, String> -> reqParser.assumeRoles = roles},
                    ReplicateIndexRequest.FGAC_ROLES_PARSER, null, ParseField("assume_roles"))
            AUTOFOLLOW_REQ_PARSER.declareObjectOrDefault(BiConsumer{ request: UpdateAutoFollowPatternRequest, settings: Settings -> request.settings = settings}, BiFunction{ p: XContentParser?, c: Void? -> Settings.fromXContent(p) },
                    null, ParseField(KEY_SETTINGS))
        }
        fun fromXContent(xcp: XContentParser, action: Action) : UpdateAutoFollowPatternRequest {
            val updateAutofollowReq = AUTOFOLLOW_REQ_PARSER.parse(xcp, null)
            updateAutofollowReq.action = action
            if(updateAutofollowReq.assumeRoles?.size == 0) {
                updateAutofollowReq.assumeRoles = null
            }
            if (updateAutofollowReq.settings == null) {
                updateAutofollowReq.settings = Settings.EMPTY
            }
            return updateAutofollowReq
        }
    }


    constructor(connection: String, patternName: String, pattern: String?, action: Action, settings: Settings) {
        this.connection = connection
        this.patternName = patternName
        this.pattern = pattern
        this.action = action
        this.settings = settings
    }

    constructor(inp: StreamInput) : super(inp) {
        connection = inp.readString()
        patternName = inp.readString()
        pattern = inp.readOptionalString()
        action = inp.readEnum(Action::class.java)
        var leaderClusterRole = inp.readOptionalString()
        var followerClusterRole = inp.readOptionalString()
        assumeRoles = HashMap()
        if(leaderClusterRole != null) assumeRoles!![ReplicateIndexRequest.LEADER_CLUSTER_ROLE] = leaderClusterRole
        if(followerClusterRole != null) assumeRoles!![ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE] = followerClusterRole
        settings = Settings.readSettingsFromStream(inp)
    }


    override fun validate(): ActionRequestValidationException? {

        var validationException = ActionRequestValidationException()
        if(!this::connection.isInitialized ||
                !this::patternName.isInitialized) {
            validationException.addValidationError("Missing connection or name in the request")
        }

        if(assumeRoles != null && (assumeRoles!!.size < 2 || assumeRoles!![ReplicateIndexRequest.LEADER_CLUSTER_ROLE] == null ||
                        assumeRoles!![ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE] == null)) {
            validationException.addValidationError("Need roles for ${ReplicateIndexRequest.LEADER_CLUSTER_ROLE} and " +
                    "${ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE}")
        }

        if(action == Action.REMOVE) {
            if(pattern != null) {
                validationException.addValidationError("Unexpected pattern")
            }
        } else if(pattern == null) {
            validationException.addValidationError("Missing pattern")
        }

        return if(validationException.validationErrors().isEmpty()) return null else validationException
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(connection)
        out.writeString(patternName)
        out.writeOptionalString(pattern)
        out.writeEnum(action)
        out.writeOptionalString(assumeRoles?.get(ReplicateIndexRequest.LEADER_CLUSTER_ROLE))
        out.writeOptionalString(assumeRoles?.get(ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE))
        Settings.writeSettingsToStream(settings, out)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field("leader_alias", connection)
        builder.field("pattern_name", patternName)
        builder.field("pattern", pattern)
        builder.field("action", action.name)
        if(assumeRoles != null) {
            builder.field("assume_roles")
            builder.startObject()
            builder.field("leader_cluster_role", assumeRoles!!.get(ReplicateIndexRequest.LEADER_CLUSTER_ROLE))
            builder.field("follower_cluster_role", assumeRoles!!.get(ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE))
            builder.endObject()
        }

        builder.startObject(KEY_SETTINGS)
        settings.toXContent(builder, ToXContent.MapParams(Collections.singletonMap("flat_settings", "true")));
        builder.endObject()

        return builder.endObject()
    }
}