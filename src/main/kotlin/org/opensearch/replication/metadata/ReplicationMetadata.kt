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

package org.opensearch.replication.metadata

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
typealias AutoFollowPatterns = Map<String, AutoFollowPattern> // { pattern name -> pattern }
typealias ReplicatedIndices = Map<String, String>             // { follower index name -> remote index name }
typealias SecurityContexts = Map<String, String>           // { follower index name -> User detail string }
typealias ClusterAlias = String
typealias ReplicationStateParams = Map<String, String>
typealias FollowIndexName = String

data class ReplicationMetadata(val autoFollowPatterns: Map<ClusterAlias, AutoFollowPatterns>,
                               val replicatedIndices: Map<ClusterAlias, ReplicatedIndices>,
                               val replicationDetails: Map<FollowIndexName, ReplicationStateParams>,
                               val securityContexts: Map<ClusterAlias, SecurityContexts>) : Metadata.Custom {

    companion object {
        const val NAME = "replication_metadata"
        const val AUTO_FOLLOW_PATTERNS_KEY = "auto_follow_patterns"
        const val REPLICATED_INDICES_KEY = "replicated_indices"
        const val REPLICATION_DETAILS_KEY = "replication_details"
        const val SECURITY_CONTEXTS_KEY = "security_contexts"
        const val AUTOFOLLOW_SECURITY_CONTEXT_PATTERN_PREFIX = "odfe_autofollow_security_context_"

        val EMPTY = ReplicationMetadata(mapOf(), mapOf(), mapOf(), mapOf())

        val patternsSerializer = object : NonDiffableValueSerializer<String, AutoFollowPatterns>() {
            override fun write(value: AutoFollowPatterns, out: StreamOutput) {
                out.writeMap(value, StreamOutput::writeString) { o, v -> v.writeTo(o) }
            }

            override fun read(inp: StreamInput, key: String): AutoFollowPatterns {
                return inp.readMap(StreamInput::readString, ::AutoFollowPattern)
            }
        }

        val indicesSerializer = object: NonDiffableValueSerializer<String, ReplicatedIndices>() {
            override fun write(value: ReplicatedIndices, out: StreamOutput) {
                out.writeMap(value, StreamOutput::writeString, StreamOutput::writeString)
            }

            override fun read(inp: StreamInput, key: String): ReplicatedIndices {
                return inp.readMap(StreamInput::readString, StreamInput::readString)
            }
        }

        val replicationDetailsSerializer = object: NonDiffableValueSerializer<String, ReplicationStateParams>() {
            override fun write(value: ReplicationStateParams, out: StreamOutput) {
                out.writeMap(value, StreamOutput::writeString, StreamOutput::writeString)
            }

            override fun read(inp: StreamInput, key: String): ReplicationStateParams {
                return inp.readMap(StreamInput::readString, StreamInput::readString)
            }
        }

        val securityContextsSerializer : NonDiffableValueSerializer<String, SecurityContexts> = indicesSerializer

        @Throws(IOException::class)
        fun fromXContent(parser: XContentParser): ReplicationMetadata {
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
                } else if (AUTO_FOLLOW_PATTERNS_KEY == currentField) {
                    val allPatterns = HashMap<String, AutoFollowPatterns>()
                    while(parser.nextToken().also { token = it } != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentField = parser.currentName()
                        }
                        else if(token == XContentParser.Token.START_OBJECT) {
                            var patternsMap = parser.mapStrings()
                            var connectionPatterns = HashMap<String, AutoFollowPattern>()
                            // It is converted to patternName -> pattern under cluster alias on disk
                            patternsMap.forEach{ (patternName, pattern) ->
                                connectionPatterns[patternName] = AutoFollowPattern(patternName, pattern)
                            }
                            allPatterns[currentField!!] = connectionPatterns
                        }
                        else {
                            throw IllegalArgumentException("Unexpected token during parsing " +
                                    "replication_metadata[$AUTO_FOLLOW_PATTERNS_KEY] - $token")
                        }
                    }
                    builder.autoFollowPatterns(allPatterns)
                } else if (REPLICATED_INDICES_KEY == currentField) {
                    val allreplicatedIndices = HashMap<String, ReplicatedIndices>()
                    while(parser.nextToken().also { token = it } != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentField = parser.currentName()
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            var replicatedIndices = parser.mapStrings()
                            allreplicatedIndices[currentField!!] = replicatedIndices
                        } else {
                            throw IllegalArgumentException("Unexpected token during parsing " +
                                    "replication_metadata[$REPLICATED_INDICES_KEY] - $token")
                        }
                    }
                    builder.replicatedIndices(allreplicatedIndices)
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
                                    "replication_metadata[$REPLICATED_INDICES_KEY] - $token")
                        }
                    }
                    builder.replicationDetails(onGoingReplicationDetails)
                } else if (SECURITY_CONTEXTS_KEY == currentField) {
                    val allSecurityContexts = HashMap<String, SecurityContexts>()
                    while(parser.nextToken().also { token = it } != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentField = parser.currentName()
                        }
                        else if(token == XContentParser.Token.START_OBJECT) {
                            var securityContexts = parser.mapStrings()
                            allSecurityContexts[currentField!!] = securityContexts
                        } else {
                            throw IllegalArgumentException("Unexpected token during parsing " +
                                    "replication_metadata[$REPLICATED_INDICES_KEY] - $token")
                        }
                    }
                    builder.securityContexts(allSecurityContexts)
                }
            }
            return builder.build()
        }
    }

    class Builder {
        private var autoFollowPattern: Map<ClusterAlias, AutoFollowPatterns> = mapOf()
        private var replicatedIndices: Map<ClusterAlias, ReplicatedIndices> = mapOf()
        private var replicationDetails: Map<FollowIndexName, ReplicationStateParams> = mapOf()
        private var securityContexts: Map<ClusterAlias, SecurityContexts> = mapOf()

        fun autoFollowPatterns(patterns: Map<String, AutoFollowPatterns>): Builder {
            this.autoFollowPattern = patterns
            return this
        }

        fun replicatedIndices(replicatedIndices: Map<String, ReplicatedIndices>): Builder {
            this.replicatedIndices = replicatedIndices
            return this
        }

        fun replicationDetails(replicationDetails: Map<String, ReplicationStateParams>): Builder {
            this.replicationDetails = replicationDetails
            return this
        }

        fun securityContexts(securityContexts: Map<String, SecurityContexts>): Builder {
            this.securityContexts = securityContexts
            return this
        }

        fun build(): ReplicationMetadata {
            return ReplicationMetadata(autoFollowPattern, replicatedIndices, replicationDetails, securityContexts)
        }
    }

    constructor(inp: StreamInput) : this(
        inp.readMap(StreamInput::readString) { i -> patternsSerializer.read(i, "") },
        inp.readMap(StreamInput::readString) { i -> indicesSerializer.read(i, "") },
        inp.readMap(StreamInput::readString) {i -> replicationDetailsSerializer.read(i, "")},
        inp.readMap(StreamInput::readString) { i -> securityContextsSerializer.read(i, "") }
    )

    override fun writeTo(out: StreamOutput) {
        out.writeMap(autoFollowPatterns, StreamOutput::writeString) { o, v ->  patternsSerializer.write(v, o) }
        out.writeMap(replicatedIndices, StreamOutput::writeString) { o, v -> indicesSerializer.write(v, o) }
        out.writeMap(replicationDetails, StreamOutput::writeString) { o, v -> replicationDetailsSerializer.write(v, o) }
        out.writeMap(securityContexts, StreamOutput::writeString) { o, v -> securityContextsSerializer.write(v, o)}
    }

    override fun diff(previousState: Metadata.Custom) = Diff(previousState as ReplicationMetadata, this)

    override fun getWriteableName(): String = NAME

    override fun getMinimalSupportedVersion(): Version = Version.V_1_0_0

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject(AUTO_FOLLOW_PATTERNS_KEY)
        autoFollowPatterns.forEach { (connectionName, patterns) ->
            builder.field(connectionName, patterns.values.associate { it.name to it.pattern })
        }
        builder.endObject()
        builder.startObject(REPLICATED_INDICES_KEY)
        replicatedIndices.forEach { (connectionName, indices) ->
            builder.field(connectionName, indices)
        }
        builder.endObject()
        builder.startObject(REPLICATION_DETAILS_KEY)
        replicationDetails.forEach { (followIndex, replicationParams) ->
            builder.field(followIndex, replicationParams)
        }
        builder.endObject()
        builder.startObject(SECURITY_CONTEXTS_KEY)
        securityContexts.forEach { (connectionName, securityContext) ->
            builder.field(connectionName, securityContext)
        }
        return builder.endObject()
    }

    override fun context(): EnumSet<Metadata.XContentContext> = Metadata.ALL_CONTEXTS

    fun removeRemoteCluster(clusterAlias: ClusterAlias) : ReplicationMetadata {
        if (clusterAlias !in autoFollowPatterns && clusterAlias !in replicatedIndices) {
            return this
        }
        return ReplicationMetadata(autoFollowPatterns.minus(clusterAlias), replicatedIndices.minus(clusterAlias),
                replicationDetails, securityContexts.minus(clusterAlias))
    }

    fun removePattern(clusterAlias: ClusterAlias, patternName: String): ReplicationMetadata {
        val currentPatterns = autoFollowPatterns.getOrDefault(clusterAlias, emptyMap())
        if (patternName !in currentPatterns) {
            return this
        }
        val newPatterns = autoFollowPatterns.plus(clusterAlias to currentPatterns.minus(patternName))
        return copy(autoFollowPatterns = newPatterns)
    }

    fun removeIndex(clusterAlias: ClusterAlias, index: String) : ReplicationMetadata {
        val currentIndices = replicatedIndices.getOrDefault(clusterAlias, emptyMap())
        if (index !in currentIndices) {
            return this
        }
        val newIndices = replicatedIndices.plus(clusterAlias to currentIndices.minus(index))
        return copy(replicatedIndices = newIndices)
    }

    fun removeSecurityContext(clusterAlias: ClusterAlias, index: String) : ReplicationMetadata {
        val currentIndices = securityContexts.getOrDefault(clusterAlias, emptyMap())
        if(index !in currentIndices) {
            return this
        }
        val newSecurityContext = securityContexts.plus(clusterAlias to currentIndices.minus(index))
        return copy(securityContexts = newSecurityContext)
    }

    fun addPattern(clusterAlias: ClusterAlias, newPattern: AutoFollowPattern) : ReplicationMetadata {
        val currentPatterns = autoFollowPatterns.getOrDefault(clusterAlias, emptyMap())
        val currentPattern = currentPatterns[newPattern.name]
        if (currentPattern == newPattern) {
            return this
        }
        val newPatterns = autoFollowPatterns.plus(clusterAlias to currentPatterns.plus(newPattern.name to newPattern))
        return copy(autoFollowPatterns = newPatterns)
    }

    fun addIndex(clusterAlias: ClusterAlias, index: String, remoteIndex: String) : ReplicationMetadata {
        val currentIndices = replicatedIndices.getOrDefault(clusterAlias, emptyMap())
        if (index in currentIndices) {
            check(currentIndices[index] == remoteIndex) {
                "$index is already replicating ${currentIndices[index]}, can't replicate $remoteIndex."
            }
            return this
        }
        val newIndices = replicatedIndices.plus(clusterAlias to currentIndices.plus(index to remoteIndex))
        return copy(replicatedIndices = newIndices)
    }

    fun addReplicationStateParams(followIndexName: String, replicationParams: ReplicationStateParams)
            : ReplicationMetadata {
        val currentStateParamsForIndex = replicationDetails.getOrDefault(followIndexName, emptyMap())
        val newStateParamsForIndex = currentStateParamsForIndex.plus(replicationParams)
        val newReplicationDetails = replicationDetails.plus(followIndexName to newStateParamsForIndex)
        return copy(replicationDetails = newReplicationDetails)
    }

    fun removeReplicationStateParams(followIndexName: String) :
            ReplicationMetadata {
        replicationDetails[followIndexName] ?: return this
        return copy(replicationDetails = replicationDetails.minus(followIndexName))
    }

    fun addSecurityContext(clusterAlias: ClusterAlias, index: String, injectedUser: String?) : ReplicationMetadata {
        val currentIndices = securityContexts.getOrDefault(clusterAlias, emptyMap())
        if((index in currentIndices && injectedUser.equals(currentIndices[index])) || injectedUser == null) {
            return this
        }
        val newSecurityContext = securityContexts.plus(clusterAlias to currentIndices.plus(index to injectedUser))
        return copy(securityContexts = newSecurityContext)
    }

    class Diff : NamedDiff<Metadata.Custom> {

        private val autoFollowPatterns : ESDiff<Map<ClusterAlias, AutoFollowPatterns>>
        private val replicatedIndices : ESDiff<Map<ClusterAlias, ReplicatedIndices>>
        private val replicationDetails : ESDiff<Map<FollowIndexName, ReplicationStateParams>>
        private val securityContexts : ESDiff<Map<ClusterAlias, SecurityContexts>>

        constructor(previous: ReplicationMetadata, current: ReplicationMetadata) {
            autoFollowPatterns = DiffableUtils.diff(previous.autoFollowPatterns, current.autoFollowPatterns,
                                                    getStringKeySerializer(), patternsSerializer)
            replicatedIndices = DiffableUtils.diff(previous.replicatedIndices, current.replicatedIndices,
                                                   getStringKeySerializer(), indicesSerializer)
            replicationDetails = DiffableUtils.diff(previous.replicationDetails, current.replicationDetails,
                                                    getStringKeySerializer(), replicationDetailsSerializer)
            securityContexts = DiffableUtils.diff(previous.securityContexts, current.securityContexts,
                                                   getStringKeySerializer(), securityContextsSerializer)
        }

        constructor(inp: StreamInput) {
            autoFollowPatterns = DiffableUtils.readJdkMapDiff(inp, getStringKeySerializer(), patternsSerializer)
            replicatedIndices = DiffableUtils.readJdkMapDiff(inp, getStringKeySerializer(), indicesSerializer)
            replicationDetails = DiffableUtils.readJdkMapDiff(inp, getStringKeySerializer(), replicationDetailsSerializer)
            securityContexts = DiffableUtils.readJdkMapDiff(inp, getStringKeySerializer(), securityContextsSerializer)
        }

        override fun writeTo(out: StreamOutput) {
            autoFollowPatterns.writeTo(out)
            replicatedIndices.writeTo(out)
            replicationDetails.writeTo(out)
            securityContexts.writeTo(out)
        }

        override fun getWriteableName() = NAME

        override fun apply(part: Metadata.Custom): Metadata.Custom {
            part as ReplicationMetadata
            return ReplicationMetadata(autoFollowPatterns.apply(part.autoFollowPatterns),
                                       replicatedIndices.apply(part.replicatedIndices),
                                        replicationDetails.apply(part.replicationDetails),
                                       securityContexts.apply(part.securityContexts))
        }
    }
}

const val REPLICATION_OVERALL_STATE_KEY = "REPLICATION_OVERALL_STATE_KEY"
const val REPLICATION_OVERALL_STATE_RUNNING_VALUE = "RUNNING"

fun getReplicationStateParamsForIndex(clusterService: ClusterService,
                                      followerIndex: String) : ReplicationStateParams? {
    return clusterService.state().metadata.custom<ReplicationMetadata>(ReplicationMetadata.NAME)
            ?.replicationDetails?.get(followerIndex)
}