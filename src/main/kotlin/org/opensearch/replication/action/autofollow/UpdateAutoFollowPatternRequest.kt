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

import org.opensearch.replication.action.index.ReplicateIndexRequest
import org.opensearch.replication.metadata.store.KEY_SETTINGS
import org.opensearch.replication.util.ValidationUtil.validateName
import org.opensearch.replication.util.ValidationUtil.validatePattern
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.support.clustermanager.AcknowledgedRequest
import org.opensearch.common.unit.TimeValue
import org.opensearch.core.ParseField
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.common.settings.Settings
import org.opensearch.core.xcontent.ObjectParser
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import java.util.Collections
import java.util.function.BiConsumer
import kotlin.collections.HashMap

class UpdateAutoFollowPatternRequest : AcknowledgedRequest<UpdateAutoFollowPatternRequest>, ToXContentObject {

    lateinit var connection: String
    lateinit var patternName: String
    var pattern: String? = null
    var useRoles: HashMap<String, String>? = null // roles to use - {leader_fgac_role: role1, follower_fgac_role: role2}
    var settings: Settings = Settings.EMPTY

    // Point 1 & 5: Persistent checkpoint configuration fields
    var checkpointPersistenceEnabled: Boolean = DEFAULT_CHECKPOINT_PERSISTENCE_ENABLED
    var checkpointRetentionPeriod: TimeValue = DEFAULT_CHECKPOINT_RETENTION_PERIOD

    // Point 3 & 4: Resume mode when a role transition (leader ↔ follower) is detected
    var roleTransitionResumeMode: RoleTransitionResumeMode = DEFAULT_ROLE_TRANSITION_RESUME_MODE

    enum class Action {
        ADD, REMOVE
    }

    /**
     * Determines how replication resumes after a role transition (Point 3 & 4).
     * CHECKPOINT: Resume from the last persisted leader/follower checkpoint — avoids full re-sync.
     * FULL_RESYNC: Ignore existing checkpoints and perform a full re-sync from scratch.
     */
    enum class RoleTransitionResumeMode {
        CHECKPOINT,
        FULL_RESYNC
    }

    lateinit var action: Action

    private constructor()

    companion object {

        // Point 5: Setting key constants aligned with the ccr.* namespace
        const val CHECKPOINT_PERSISTENCE_ENABLED_KEY = "ccr.checkpoint.persistence.enabled"
        const val CHECKPOINT_RETENTION_PERIOD_KEY    = "ccr.checkpoint.retention_period"
        const val ROLE_TRANSITION_RESUME_MODE_KEY    = "ccr.role_transition.resume_mode"

        // Sensible defaults
        const val DEFAULT_CHECKPOINT_PERSISTENCE_ENABLED = true
        val DEFAULT_CHECKPOINT_RETENTION_PERIOD: TimeValue = TimeValue.timeValueHours(24)
        val DEFAULT_ROLE_TRANSITION_RESUME_MODE: RoleTransitionResumeMode = RoleTransitionResumeMode.CHECKPOINT

        private val AUTOFOLLOW_REQ_PARSER =
            ObjectParser<UpdateAutoFollowPatternRequest, Void>("AutoFollowRequestParser") {
                UpdateAutoFollowPatternRequest()
            }

        init {
            AUTOFOLLOW_REQ_PARSER.declareString(
                UpdateAutoFollowPatternRequest::connection::set, ParseField("leader_alias"))
            AUTOFOLLOW_REQ_PARSER.declareString(
                UpdateAutoFollowPatternRequest::patternName::set, ParseField("name"))
            AUTOFOLLOW_REQ_PARSER.declareString(
                UpdateAutoFollowPatternRequest::pattern::set, ParseField("pattern"))

            AUTOFOLLOW_REQ_PARSER.declareObjectOrDefault(
                BiConsumer { reqParser: UpdateAutoFollowPatternRequest, roles: HashMap<String, String> ->
                    reqParser.useRoles = roles
                },
                ReplicateIndexRequest.FGAC_ROLES_PARSER, null, ParseField("use_roles"))

            AUTOFOLLOW_REQ_PARSER.declareObjectOrDefault(
                { request: UpdateAutoFollowPatternRequest, s: Settings -> request.settings = s },
                { p: XContentParser?, _: Void? -> Settings.fromXContent(p) },
                null, ParseField(KEY_SETTINGS))

            // Point 5: Parse checkpoint persistence configuration from REST request body
            AUTOFOLLOW_REQ_PARSER.declareBoolean(
                { req, v -> req.checkpointPersistenceEnabled = v },
                ParseField(CHECKPOINT_PERSISTENCE_ENABLED_KEY))

            AUTOFOLLOW_REQ_PARSER.declareString(
                { req, v -> req.checkpointRetentionPeriod =
                    TimeValue.parseTimeValue(v, CHECKPOINT_RETENTION_PERIOD_KEY) },
                ParseField(CHECKPOINT_RETENTION_PERIOD_KEY))

            AUTOFOLLOW_REQ_PARSER.declareString(
                { req, v -> req.roleTransitionResumeMode =
                    RoleTransitionResumeMode.valueOf(v.uppercase()) },
                ParseField(ROLE_TRANSITION_RESUME_MODE_KEY))
        }

        fun fromXContent(xcp: XContentParser, action: Action): UpdateAutoFollowPatternRequest {
            val updateAutofollowReq = AUTOFOLLOW_REQ_PARSER.parse(xcp, null)
            updateAutofollowReq.action = action
            if (updateAutofollowReq.useRoles?.size == 0) {
                updateAutofollowReq.useRoles = null
            }
            return updateAutofollowReq
        }
    }

    // Point 5: Expose new checkpoint fields as optional parameters with backward-compatible defaults
    constructor(
        connection: String,
        patternName: String,
        pattern: String?,
        action: Action,
        settings: Settings,
        checkpointPersistenceEnabled: Boolean = DEFAULT_CHECKPOINT_PERSISTENCE_ENABLED,
        checkpointRetentionPeriod: TimeValue = DEFAULT_CHECKPOINT_RETENTION_PERIOD,
        roleTransitionResumeMode: RoleTransitionResumeMode = DEFAULT_ROLE_TRANSITION_RESUME_MODE
    ) {
        this.connection = connection
        this.patternName = patternName
        this.pattern = pattern
        this.action = action
        this.settings = settings
        this.checkpointPersistenceEnabled = checkpointPersistenceEnabled
        this.checkpointRetentionPeriod = checkpointRetentionPeriod
        this.roleTransitionResumeMode = roleTransitionResumeMode
    }

    constructor(inp: StreamInput) : super(inp) {
        connection = inp.readString()
        patternName = inp.readString()
        pattern = inp.readOptionalString()
        action = inp.readEnum(Action::class.java)

        val leaderClusterRole = inp.readOptionalString()
        val followerClusterRole = inp.readOptionalString()
        useRoles = HashMap()
        if (leaderClusterRole != null) useRoles!![ReplicateIndexRequest.LEADER_CLUSTER_ROLE] = leaderClusterRole
        if (followerClusterRole != null) useRoles!![ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE] = followerClusterRole
        // Bug fix: null useRoles serialises as two null optional strings and deserialises as an empty
        // HashMap, which incorrectly triggers the "size < 2" validation error on the receiving node.
        // Restore null to stay consistent with fromXContent behaviour.
        if (useRoles!!.isEmpty()) useRoles = null

        settings = Settings.readSettingsFromStream(inp)

        // Point 1 & 2: Deserialize persistent checkpoint configuration
        checkpointPersistenceEnabled = inp.readBoolean()
        checkpointRetentionPeriod    = inp.readTimeValue()
        roleTransitionResumeMode     = inp.readEnum(RoleTransitionResumeMode::class.java)
    }

    override fun validate(): ActionRequestValidationException? {
        val validationException = ActionRequestValidationException()

        if (!this::connection.isInitialized || !this::patternName.isInitialized) {
            validationException.addValidationError("Missing connection or name in the request")
        }

        validateName(patternName, validationException)

        if (useRoles != null && (useRoles!!.size < 2 ||
                    useRoles!![ReplicateIndexRequest.LEADER_CLUSTER_ROLE] == null ||
                    useRoles!![ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE] == null)) {
            validationException.addValidationError(
                "Need roles for ${ReplicateIndexRequest.LEADER_CLUSTER_ROLE} and " +
                        "${ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE}")
        }

        if (action == Action.REMOVE) {
            if (pattern != null) validationException.addValidationError("Unexpected pattern")
        } else {
            if (pattern == null)
                validationException.addValidationError("Missing pattern")
            else
                validatePattern(pattern, validationException)
        }

        // Point 5: Validate that retention period is a positive duration
        if (checkpointRetentionPeriod.millis <= 0) {
            validationException.addValidationError(
                "$CHECKPOINT_RETENTION_PERIOD_KEY must be a positive duration")
        }

        return if (validationException.validationErrors().isEmpty()) null else validationException
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(connection)
        out.writeString(patternName)
        out.writeOptionalString(pattern)
        out.writeEnum(action)
        out.writeOptionalString(useRoles?.get(ReplicateIndexRequest.LEADER_CLUSTER_ROLE))
        out.writeOptionalString(useRoles?.get(ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE))
        Settings.writeSettingsToStream(settings, out)
        // Point 1 & 2: Serialize persistent checkpoint configuration
        out.writeBoolean(checkpointPersistenceEnabled)
        out.writeTimeValue(checkpointRetentionPeriod)
        out.writeEnum(roleTransitionResumeMode)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field("leader_alias", connection)
        builder.field("pattern_name", patternName)
        builder.field("pattern", pattern)
        builder.field("action", action.name)

        if (useRoles != null) {
            builder.startObject("use_roles")
            builder.field("leader_cluster_role", useRoles!![ReplicateIndexRequest.LEADER_CLUSTER_ROLE])
            builder.field("follower_cluster_role", useRoles!![ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE])
            builder.endObject()
        }

        builder.startObject(KEY_SETTINGS)
        settings.toXContent(builder, ToXContent.MapParams(Collections.singletonMap("flat_settings", "true")))
        builder.endObject()

        // Point 1 & 5: Expose checkpoint configuration in REST response
        builder.field(CHECKPOINT_PERSISTENCE_ENABLED_KEY, checkpointPersistenceEnabled)
        builder.field(CHECKPOINT_RETENTION_PERIOD_KEY, checkpointRetentionPeriod.stringRep)
        builder.field(ROLE_TRANSITION_RESUME_MODE_KEY, roleTransitionResumeMode.name.lowercase())

        return builder.endObject()
    }
}