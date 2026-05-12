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

import org.opensearch.replication.metadata.store.ReplicationContext
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.support.clustermanager.AcknowledgedRequest
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder

class SetupChecksRequest: AcknowledgedRequest<SetupChecksRequest>, ToXContentObject {
    val followerContext: ReplicationContext
    val leaderContext: ReplicationContext
    val connectionName: String

    constructor(followerContext: ReplicationContext,
                leaderContext: ReplicationContext,
                connectionName: String) {
        this.followerContext = followerContext
        this.leaderContext = leaderContext
        this.connectionName = connectionName
    }

    constructor(inp: StreamInput): super(inp) {
        this.followerContext = ReplicationContext(inp)
        this.leaderContext = ReplicationContext(inp)
        this.connectionName = inp.readString()
    }

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        followerContext.writeTo(out)
        leaderContext.writeTo(out)
        out.writeString(connectionName)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field("follower_context", this.followerContext)
        builder.field("leader_context", this.leaderContext)
        builder.field("connection_name", this.connectionName)
        return builder.endObject()
    }
}