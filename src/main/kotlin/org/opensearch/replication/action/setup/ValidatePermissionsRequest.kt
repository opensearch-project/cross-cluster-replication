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

package org.opensearch.replication.action.setup

import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.IndicesRequest
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.clustermanager.AcknowledgedRequest
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder

class ValidatePermissionsRequest: AcknowledgedRequest<ValidatePermissionsRequest>, IndicesRequest.Replaceable, ToXContentObject {
    val cluster: String
    val index: String
    var clusterRole: String? = null


    constructor(leaderAlias: String, leaderIndex: String, leaderClusterRole: String?) {
        this.cluster = leaderAlias
        this.index = leaderIndex
        this.clusterRole = leaderClusterRole
    }

    constructor(inp: StreamInput) : super(inp) {
        cluster = inp.readString()
        index = inp.readString()
        clusterRole = inp.readOptionalString()
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(cluster)
        out.writeString(index)
        out.writeOptionalString(clusterRole)
    }

    override fun validate(): ActionRequestValidationException? {
        // Request only accepts non-null string for leaderIndex
        return null
    }

    override fun indices(vararg indices: String?): IndicesRequest {
        return this
    }

    override fun indices(): Array<String> {
        return arrayOf(index)
    }

    override fun indicesOptions(): IndicesOptions {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed()
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field("cluster", cluster)
        builder.field("resource", index)
        builder.field("cluster_role", clusterRole)
        return builder.endObject()
    }
}
