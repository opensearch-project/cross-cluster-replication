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

package org.opensearch.replication.task.bulk

import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.replication.metadata.state.BulkTaskState
import org.opensearch.tasks.Task

data class FailedIndex(val index: String, val reason: String)

class BulkReplicationTaskStatus(
    val operationType: String,
    val pattern: String,
    val startTime: Long,
    val numSuccess: Int,
    val numFailed: Int,
    val numPending: Int,
    val numCancelled: Int,
    val failedIndices: List<FailedIndex>
) : Task.Status {

    companion object {
        const val NAME = "bulk_replication_task_status"

        fun fromTaskState(s: BulkTaskState) = BulkReplicationTaskStatus(
            s.operationType, s.pattern, s.startTime,
            s.numSuccess, s.numFailed, s.numPending, s.numCancelled,
            s.failedIndices
        )
    }

    constructor(inp: StreamInput) : this(
        operationType = inp.readString(), pattern = inp.readString(), startTime = inp.readLong(),
        numSuccess = inp.readInt(), numFailed = inp.readInt(), numPending = inp.readInt(), numCancelled = inp.readInt(),
        failedIndices = inp.readList { i -> FailedIndex(i.readString(), i.readString()) }
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(operationType)
        out.writeString(pattern)
        out.writeLong(startTime)
        out.writeInt(numSuccess)
        out.writeInt(numFailed)
        out.writeInt(numPending)
        out.writeInt(numCancelled)
        out.writeCollection(failedIndices) { o, fi -> o.writeString(fi.index); o.writeString(fi.reason) }
    }

    override fun getWriteableName() = NAME

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
        builder.startObject()
        builder.field("operation_type", operationType)
        builder.field("pattern", pattern)
        builder.field("start_time", startTime)
        builder.field("num_success", numSuccess)
        builder.field("num_failed", numFailed)
        builder.field("num_pending", numPending)
        builder.field("num_cancelled", numCancelled)
        builder.startArray("failed_indices")
        failedIndices.forEach { fi ->
            builder.startObject().field("index", fi.index).field("failure_reason", fi.reason).endObject()
        }
        builder.endArray()
        return builder.endObject()
    }
}
