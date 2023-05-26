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

package org.opensearch.replication.task

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentFragment
import org.opensearch.core.xcontent.XContentBuilder

/**
 * Enum that represents the state of replication of either shards or indices.
 */
enum class ReplicationState : Writeable, ToXContentFragment {

    INIT, RESTORING, INIT_FOLLOW, FOLLOWING, MONITORING, FAILED, COMPLETED;

    override fun writeTo(out: StreamOutput) {
        out.writeEnum(this)
        fun readState(inp : StreamInput) : ReplicationState = inp.readEnum(ReplicationState::class.java)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
        return builder.value(toString())
    }
}