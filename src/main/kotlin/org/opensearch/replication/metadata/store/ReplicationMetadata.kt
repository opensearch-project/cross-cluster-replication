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

package org.opensearch.replication.metadata.store

import org.opensearch.commons.authuser.User
import org.opensearch.core.ParseField
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.common.settings.Settings
import org.opensearch.core.xcontent.ObjectParser
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import java.io.IOException
import java.util.*
import java.util.function.BiConsumer
import java.util.function.BiFunction

const val KEY_SETTINGS = "settings"

class ReplicationContext: ToXContent, Writeable {
    lateinit var resource: String
    var user: User? = null
    var isSecurityContextEnabled: Boolean = false

    private constructor() {
    }

    constructor(resource: String) {
        this.resource = resource
    }

    constructor(resource: String, user: User?) {
        this.resource = resource
        this.user = user
        if(user != null) isSecurityContextEnabled = true
    }

    constructor(inp: StreamInput) {
        resource = inp.readString()
        isSecurityContextEnabled = inp.readBoolean()
        if(isSecurityContextEnabled) User(inp)
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(resource)
        out.writeBoolean(isSecurityContextEnabled)
        if(isSecurityContextEnabled) user!!.writeTo(out)
    }

    companion object {
        val REPLICATION_CONTEXT_PARSER = ObjectParser<ReplicationContext, Void>("ReplicationContextParser") { ReplicationContext() }
        init {
            REPLICATION_CONTEXT_PARSER.declareString(ReplicationContext::resource::set, ParseField("resource"))
            REPLICATION_CONTEXT_PARSER.declareObjectOrDefault(
                { replicationContext: ReplicationContext, user: User -> replicationContext.user = user},
                {parser: XContentParser, _ -> User.parse(parser)},
                null, ParseField("user"))
            //REPLICATION_CONTEXT_PARSER.declareField({ parser, _, _ -> User.parse(parser)}, ParseField("user"), ObjectParser.ValueType.OBJECT)
        }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field("resource", resource)
        builder.field("user", user)
        return builder.endObject()
    }
}

class ReplicationMetadata: ToXContent {
    lateinit var connectionName: String
    lateinit var metadataType: String
    lateinit var overallState: String
    lateinit var reason: String
    lateinit var followerContext: ReplicationContext
    lateinit var leaderContext: ReplicationContext
    lateinit var settings: Settings

    // Point 1 & 2: Bidirectional checkpoint tracking — persisted so role transitions can resume
    var leaderSequenceNumber: Long = UNASSIGNED_SEQ_NO
    var followerAppliedSequence: Long = UNASSIGNED_SEQ_NO

    // Point 3 & 5: Checkpoint configuration stored alongside metadata
    var checkpointPersistenceEnabled: Boolean = true
    var checkpointRetentionPeriod: String = "24h"
    var roleTransitionResumeMode: String = "CHECKPOINT"

    constructor(connectionName: String,
                metadataType: String,
                overallState: String,
                reason: String,
                followerContext: ReplicationContext,
                leaderContext: ReplicationContext,
                settings: Settings,
                checkpointPersistenceEnabled: Boolean = true,
                checkpointRetentionPeriod: String = "24h",
                roleTransitionResumeMode: String = "CHECKPOINT") {
        this.connectionName = connectionName
        this.metadataType = metadataType
        this.overallState = overallState
        this.reason = reason
        this.followerContext = followerContext
        this.leaderContext = leaderContext
        this.settings = settings
        this.checkpointPersistenceEnabled = checkpointPersistenceEnabled
        this.checkpointRetentionPeriod = checkpointRetentionPeriod
        this.roleTransitionResumeMode = roleTransitionResumeMode
    }

    private constructor() {
    }

    companion object {
        const val UNASSIGNED_SEQ_NO = -1L

        private val METADATA_PARSER = ObjectParser<ReplicationMetadata, Void>("ReplicationMetadataParser") { ReplicationMetadata() }
        init {
            METADATA_PARSER.declareString(ReplicationMetadata::connectionName::set, ParseField( "connection_name"))
            METADATA_PARSER.declareString(ReplicationMetadata::metadataType::set, ParseField( "metadata_type"))
            METADATA_PARSER.declareString(ReplicationMetadata::overallState::set, ParseField( "overall_state"))
            METADATA_PARSER.declareString(ReplicationMetadata::reason::set, ParseField( "reason"))
            METADATA_PARSER.declareObject(BiConsumer { metadata: ReplicationMetadata, context: ReplicationContext -> metadata.followerContext = context},
                    ReplicationContext.REPLICATION_CONTEXT_PARSER, ParseField("follower_context"))
            METADATA_PARSER.declareObject(BiConsumer { metadata: ReplicationMetadata, context: ReplicationContext -> metadata.leaderContext = context},
                    ReplicationContext.REPLICATION_CONTEXT_PARSER, ParseField("leader_context"))
            METADATA_PARSER.declareObject({ metadata: ReplicationMetadata, settings: Settings -> metadata.settings = settings},
                { p: XContentParser?, _: Void? -> Settings.fromXContent(p) },
                    ParseField(KEY_SETTINGS))
            // Point 1 & 2: Checkpoint sequence tracking fields
            METADATA_PARSER.declareLong(ReplicationMetadata::leaderSequenceNumber::set, ParseField("leader_sequence_number"))
            METADATA_PARSER.declareLong(ReplicationMetadata::followerAppliedSequence::set, ParseField("follower_applied_sequence"))
            // Point 3 & 5: Checkpoint configuration fields
            METADATA_PARSER.declareBoolean(ReplicationMetadata::checkpointPersistenceEnabled::set, ParseField("checkpoint_persistence_enabled"))
            METADATA_PARSER.declareString(ReplicationMetadata::checkpointRetentionPeriod::set, ParseField("checkpoint_retention_period"))
            METADATA_PARSER.declareString(ReplicationMetadata::roleTransitionResumeMode::set, ParseField("role_transition_resume_mode"))
        }

        @Throws(IOException::class)
        fun fromXContent(parser: XContentParser): ReplicationMetadata {
            return METADATA_PARSER.parse(parser, null)
        }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field("connection_name", connectionName)
        builder.field("metadata_type", metadataType)
        builder.field("overall_state", overallState)
        builder.field("reason", reason)

        builder.field("follower_context")
        builder.startObject()
        builder.field("resource", followerContext.resource)
        if(followerContext.user != null)
            builder.field("user", followerContext.user)
        builder.endObject()

        builder.field("leader_context")
        builder.startObject()
        builder.field("resource", leaderContext.resource)
        if(leaderContext.user != null)
            builder.field("user", leaderContext.user)
        builder.endObject()

        builder.startObject(KEY_SETTINGS)
        settings.toXContent(builder, ToXContent.MapParams(Collections.singletonMap("flat_settings", "true")));
        builder.endObject()

        // Point 1 & 2: Persist bidirectional checkpoint sequences
        builder.field("leader_sequence_number", leaderSequenceNumber)
        builder.field("follower_applied_sequence", followerAppliedSequence)
        // Point 3 & 5: Persist checkpoint configuration
        builder.field("checkpoint_persistence_enabled", checkpointPersistenceEnabled)
        builder.field("checkpoint_retention_period", checkpointRetentionPeriod)
        builder.field("role_transition_resume_mode", roleTransitionResumeMode)

        builder.endObject()

        return builder
    }

    override fun toString(): String {
        return "ReplicationMetadata - [connection_name: $connectionName, metadata_type: $metadataType, " +
                "overall_state: $overallState, reason: $reason, follower_context: ${followerContext.resource}, leader_context: ${leaderContext.resource}, " +
                " settings: ${settings} ]"
    }
}
