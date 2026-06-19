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

package org.opensearch.replication.action.bulk

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.core.ParseField
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ObjectParser
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.replication.action.index.ReplicateIndexRequest

class BulkReplicationRequest : ActionRequest {

    lateinit var pattern: String
    var excludeIndices: List<String> = emptyList()
    var leaderAlias: String? = null
    var useRoles: HashMap<String, String>? = null

    constructor(pattern: String, excludeIndices: List<String> = emptyList()) {
        this.pattern = pattern
        this.excludeIndices = excludeIndices
    }

    constructor()

    constructor(inp: StreamInput) : super(inp) {
        pattern = inp.readString()
        leaderAlias = inp.readOptionalString()
        excludeIndices = inp.readStringList()
        val leaderRole = inp.readOptionalString()
        val followerRole = inp.readOptionalString()
        if (leaderRole != null || followerRole != null) {
            useRoles = HashMap()
            if (leaderRole != null) useRoles!![ReplicateIndexRequest.LEADER_CLUSTER_ROLE] = leaderRole
            if (followerRole != null) useRoles!![ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE] = followerRole
        }
    }

    companion object {
        private val PARSER = ObjectParser<BulkReplicationRequest, Void>("BulkReplicationRequestParser") {
            BulkReplicationRequest()
        }

        init {
            PARSER.declareString(BulkReplicationRequest::pattern::set, ParseField("pattern"))
            PARSER.declareString(BulkReplicationRequest::leaderAlias::set, ParseField("leader_alias"))
            PARSER.declareObject(
                { req, roles: HashMap<String, String> -> req.useRoles = roles },
                ReplicateIndexRequest.FGAC_ROLES_PARSER,
                ParseField("use_roles")
            )
            PARSER.declareObject(
                { req, filters: Map<String, Any> ->
                    @Suppress("UNCHECKED_CAST")
                    req.excludeIndices = (filters["exclude_index"] as? List<String>) ?: emptyList()
                },
                { parser: XContentParser, _: Void? -> parser.map() },
                ParseField("filters")
            )
        }

        fun fromXContent(parser: XContentParser): BulkReplicationRequest = PARSER.parse(parser, null)
    }

    override fun validate(): ActionRequestValidationException? {
        val errors = mutableListOf<String>()
        if (!this::pattern.isInitialized || pattern.isBlank()) {
            errors.add("pattern is required")
        } else if (!isValidIndexPattern(pattern)) {
            errors.add("pattern contains invalid characters: $pattern")
        }
        if (errors.isNotEmpty()) {
            val e = ActionRequestValidationException()
            errors.forEach { e.addValidationError(it) }
            return e
        }
        return null
    }

    private fun isValidIndexPattern(pattern: String): Boolean {
        val invalidChars = setOf(' ', '"', '<', '>', '|', '\\', '#', ',')
        return pattern.none { it in invalidChars } && !pattern.startsWith("_")
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(pattern)
        out.writeOptionalString(leaderAlias)
        out.writeStringCollection(excludeIndices)
        out.writeOptionalString(useRoles?.get(ReplicateIndexRequest.LEADER_CLUSTER_ROLE))
        out.writeOptionalString(useRoles?.get(ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE))
    }
}
