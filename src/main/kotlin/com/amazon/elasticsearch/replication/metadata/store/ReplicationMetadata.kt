package com.amazon.elasticsearch.replication.metadata.store

import org.elasticsearch.common.ParseField
import org.elasticsearch.common.xcontent.ObjectParser
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import java.io.IOException
import java.util.function.BiConsumer

class ReplicationContext {
    lateinit var resource: String
    var user: String? = null
    var roles: String? = null
    var backendRoles: String? = null

    constructor() {
    }

    constructor(resource: String) {
        this.resource = resource
    }

    constructor(resource: String, user: String, roles: String, backendRoles: String) {
        this.resource = resource
        this.user = user
        this.roles = roles
        this.backendRoles = backendRoles
    }
}

class ReplicationMetadata: ToXContent {
    lateinit var connectionName: String
    lateinit var metadataType: String
    lateinit var overallState: String
    lateinit var followerContext: ReplicationContext
    lateinit var leaderContext: ReplicationContext

    constructor(connectionName: String,
                metadataType: String,
                overallState: String,
                followerContext: ReplicationContext,
                leaderContext: ReplicationContext) {
        this.connectionName = connectionName
        this.metadataType = metadataType
        this.overallState = overallState
        this.followerContext = followerContext
        this.leaderContext = leaderContext
    }

    private constructor() {
    }

    companion object {
        private val METADATA_PARSER = ObjectParser<ReplicationMetadata, Void>("ReplicationMetadataParser") { ReplicationMetadata() }
        private val REPLICATION_CONTEXT_PARSER = ObjectParser<ReplicationContext, Void>("ReplicationContextParser") { ReplicationContext() }
        init {
            REPLICATION_CONTEXT_PARSER.declareString(ReplicationContext::resource::set, ParseField("resource"))
            REPLICATION_CONTEXT_PARSER.declareStringOrNull(ReplicationContext::user::set, ParseField("user"))
            REPLICATION_CONTEXT_PARSER.declareStringOrNull(ReplicationContext::roles::set, ParseField("roles"))
            REPLICATION_CONTEXT_PARSER.declareStringOrNull(ReplicationContext::backendRoles::set, ParseField("backend_roles"))


            METADATA_PARSER.declareString(ReplicationMetadata::connectionName::set, ParseField( "connection_name"))
            METADATA_PARSER.declareString(ReplicationMetadata::metadataType::set, ParseField( "metadata_type"))
            METADATA_PARSER.declareString(ReplicationMetadata::overallState::set, ParseField( "overall_state"))
            METADATA_PARSER.declareObject(BiConsumer { metadata: ReplicationMetadata, context: ReplicationContext -> metadata.followerContext = context},
                    REPLICATION_CONTEXT_PARSER, ParseField("follower_context"))
            METADATA_PARSER.declareObject(BiConsumer { metadata: ReplicationMetadata, context: ReplicationContext -> metadata.leaderContext = context},
                    REPLICATION_CONTEXT_PARSER, ParseField("leader_context"))
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

        builder.field("follower_context")
        builder.startObject()
        builder.field("resource", followerContext.resource)
        builder.field("user", followerContext.user)
        builder.field("roles", followerContext.roles)
        builder.field("backend_roles", followerContext.backendRoles)
        builder.endObject()

        builder.field("leader_context")
        builder.startObject()
        builder.field("resource", leaderContext.resource)
        builder.field("user", leaderContext.user)
        builder.field("roles", leaderContext.roles)
        builder.field("backend_roles", leaderContext.backendRoles)
        builder.endObject()

        builder.endObject()

        return builder
    }

    override fun toString(): String {
        return "ReplicationMetadata - [connection_name: $connectionName, metadata_type: $metadataType, " +
                "overall_state: $overallState, follower_context: ${followerContext.resource}, leader_context: ${leaderContext.resource}]"
    }
}
