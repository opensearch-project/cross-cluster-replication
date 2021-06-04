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

package org.opensearch.replication.task

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentFragment
import org.opensearch.common.xcontent.XContentBuilder

/**
 * Enum that represents the state of replication of either shards or indices.
 */
enum class ReplicationState : Writeable, ToXContentFragment {

    INIT, RESTORING, INIT_FOLLOW, FOLLOWING, MONITORING, FAILED, COMPLETED; // TODO: Add PAUSED state

    override fun writeTo(out: StreamOutput) {
        out.writeEnum(this)
        fun readState(inp : StreamInput) : ReplicationState = inp.readEnum(ReplicationState::class.java)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
        return builder.value(toString())
    }
}