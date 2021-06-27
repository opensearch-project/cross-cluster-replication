package com.amazon.elasticsearch.replication.metadata.store

import com.amazon.opendistroforelasticsearch.commons.authuser.User
import org.elasticsearch.common.ParseField
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ObjectParser
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import java.io.IOException
import java.util.function.BiConsumer
import java.util.function.BiFunction

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
            REPLICATION_CONTEXT_PARSER.declareObjectOrDefault(BiConsumer{replicationContext: ReplicationContext, user: User -> replicationContext.user = user},
                    BiFunction {parser: XContentParser, _ -> User.parse(parser)}, null, ParseField("user"))
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
        init {
            METADATA_PARSER.declareString(ReplicationMetadata::connectionName::set, ParseField( "connection_name"))
            METADATA_PARSER.declareString(ReplicationMetadata::metadataType::set, ParseField( "metadata_type"))
            METADATA_PARSER.declareString(ReplicationMetadata::overallState::set, ParseField( "overall_state"))
            METADATA_PARSER.declareObject(BiConsumer { metadata: ReplicationMetadata, context: ReplicationContext -> metadata.followerContext = context},
                    ReplicationContext.REPLICATION_CONTEXT_PARSER, ParseField("follower_context"))
            METADATA_PARSER.declareObject(BiConsumer { metadata: ReplicationMetadata, context: ReplicationContext -> metadata.leaderContext = context},
                    ReplicationContext.REPLICATION_CONTEXT_PARSER, ParseField("leader_context"))
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
        if(followerContext.user != null)
            builder.field("user", followerContext.user)
        builder.endObject()

        builder.field("leader_context")
        builder.startObject()
        builder.field("resource", leaderContext.resource)
        if(leaderContext.user != null)
            builder.field("user", leaderContext.user)
        builder.endObject()

        builder.endObject()

        return builder
    }

    override fun toString(): String {
        return "ReplicationMetadata - [connection_name: $connectionName, metadata_type: $metadataType, " +
                "overall_state: $overallState, follower_context: ${followerContext.resource}, leader_context: ${leaderContext.resource}]"
    }
}
