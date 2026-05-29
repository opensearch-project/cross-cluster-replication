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

import org.opensearch.core.action.ActionResponse
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder

class BulkReplicationResponse : ActionResponse, ToXContentObject {

    val acknowledged: Boolean
    val taskId: String

    constructor(acknowledged: Boolean, taskId: String) {
        this.acknowledged = acknowledged
        this.taskId = taskId
    }

    constructor(inp: StreamInput) : super(inp) {
        acknowledged = inp.readBoolean()
        taskId = inp.readString()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeBoolean(acknowledged)
        out.writeString(taskId)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field("acknowledged", acknowledged)
            .field("task_id", taskId)
            .endObject()
    }
}
