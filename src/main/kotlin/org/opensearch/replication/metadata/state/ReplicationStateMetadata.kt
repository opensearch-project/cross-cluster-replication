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

package org.opensearch.replication.metadata.state

import org.opensearch.Version
import org.opensearch.cluster.DiffableUtils
import org.opensearch.cluster.DiffableUtils.NonDiffableValueSerializer
import org.opensearch.cluster.DiffableUtils.getStringKeySerializer
import org.opensearch.cluster.NamedDiff
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import java.io.IOException
import java.util.EnumSet
import kotlin.collections.HashMap
import org.opensearch.cluster.Diff as ESDiff

// some descriptive type aliases to make it easier to read the code.
typealias ClusterAlias = String
typealias ReplicationStateParams = Map<String, String>
typealias FollowIndexName = String

data class ReplicationStateMetadata(val replicationDetails: Map<FollowIndexName, ReplicationStateParams>) : Metadata.Custom {

    companion object {
        const val NAME = "replication_metadata"
        const val REPLICATION_DETAILS_KEY = "replication_details"

        val EMPTY = ReplicationStateMetadata(mapOf())

        val replicationDetailsSerializer = object: NonDiffableValueSerializer<String, ReplicationStateParams>() {
            override fun write(value: ReplicationStateParams, out: StreamOutput) {
                out.writeMap(value, StreamOutput::writeString, StreamOutput::writeString)
            }

            override fun read(inp: StreamInput, key: String): ReplicationStateParams {
                return inp.readMap(StreamInput::readString, StreamInput::readString)
            }
        }

        @Throws(IOException::class)
        fun fromXContent(parser: XContentParser): ReplicationStateMetadata {
            var builder = Builder()
            if (parser.currentToken() == null) {
                parser.nextToken()
            }
            var token = parser.currentToken()
            require(token == XContentParser.Token.START_OBJECT) { "expected start object but got a $token"}

            var currentField: String? = null
            while (parser.nextToken().also { token = it } !== XContentParser.Token.END_OBJECT) {
                if(token == XContentParser.Token.FIELD_NAME) {
                    currentField = parser.currentName()
                } else if (REPLICATION_DETAILS_KEY == currentField) {
                    val onGoingReplicationDetails = HashMap<String, ReplicationStateParams>()
                    while(parser.nextToken().also { token = it } != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentField = parser.currentName()
                        }
                        else if(token == XContentParser.Token.START_OBJECT) {
                            var replicationDetails = parser.mapStrings()
                            onGoingReplicationDetails[currentField!!] = replicationDetails
                        } else {
                            throw IllegalArgumentException("Unexpected token during parsing " +
                                    "replication_metadata[$REPLICATION_DETAILS_KEY] - $token")
                        }
                    }
                    builder.replicationDetails(onGoingReplicationDetails)
                }
            }
            return builder.build()
        }
    }

    class Builder {
        private var replicationDetails: Map<FollowIndexName, ReplicationStateParams> = mapOf()

        fun replicationDetails(replicationDetails: Map<String, ReplicationStateParams>): Builder {
            this.replicationDetails = replicationDetails
            return this
        }

        fun build(): ReplicationStateMetadata {
            return ReplicationStateMetadata(replicationDetails)
        }
    }

    constructor(inp: StreamInput) : this(inp.readMap(StreamInput::readString) {i -> replicationDetailsSerializer.read(i, "")})

    override fun writeTo(out: StreamOutput) {
        out.writeMap(replicationDetails, StreamOutput::writeString) { o, v -> replicationDetailsSerializer.write(v, o) }
    }

    override fun diff(previousState: Metadata.Custom) = Diff(previousState as ReplicationStateMetadata, this)

    override fun getWriteableName(): String = NAME

    override fun getMinimalSupportedVersion(): Version = Version.V_2_0_0

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject(REPLICATION_DETAILS_KEY)
        replicationDetails.forEach { (followIndex, replicationParams) ->
            builder.field(followIndex, replicationParams)
        }
        return builder.endObject()
    }

    override fun context(): EnumSet<Metadata.XContentContext> = Metadata.ALL_CONTEXTS

    fun addReplicationStateParams(followIndexName: String, replicationParams: ReplicationStateParams)
            : ReplicationStateMetadata {
        val currentStateParamsForIndex = replicationDetails.getOrDefault(followIndexName, emptyMap())
        val newStateParamsForIndex = currentStateParamsForIndex.plus(replicationParams)
        val newReplicationDetails = replicationDetails.plus(followIndexName to newStateParamsForIndex)
        return copy(replicationDetails = newReplicationDetails)
    }

    fun removeReplicationStateParams(followIndexName: String) :
            ReplicationStateMetadata {
        replicationDetails[followIndexName] ?: return this
        return copy(replicationDetails = replicationDetails.minus(followIndexName))
    }

    class Diff : NamedDiff<Metadata.Custom> {
        private val replicationDetails : ESDiff<Map<FollowIndexName, ReplicationStateParams>>

        constructor(previous: ReplicationStateMetadata, current: ReplicationStateMetadata) {
            replicationDetails = DiffableUtils.diff(previous.replicationDetails, current.replicationDetails,
                                                    getStringKeySerializer(), replicationDetailsSerializer)
        }

        constructor(inp: StreamInput) {
            replicationDetails = DiffableUtils.readJdkMapDiff(inp, getStringKeySerializer(), replicationDetailsSerializer)
        }

        override fun writeTo(out: StreamOutput) {
            replicationDetails.writeTo(out)
        }

        override fun getWriteableName() = NAME

        override fun apply(part: Metadata.Custom): Metadata.Custom {
            part as ReplicationStateMetadata
            return ReplicationStateMetadata(replicationDetails.apply(part.replicationDetails))
        }
    }
}

const val REPLICATION_LAST_KNOWN_OVERALL_STATE = "REPLICATION_LAST_KNOWN_OVERALL_STATE"

fun getReplicationStateParamsForIndex(clusterService: ClusterService,
                                      followerIndex: String) : ReplicationStateParams? {
    return clusterService.state().metadata.custom<ReplicationStateMetadata>(ReplicationStateMetadata.NAME)
            ?.replicationDetails?.get(followerIndex)
}
