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
import org.opensearch.cluster.NamedDiff
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.replication.task.bulk.FailedIndex
import java.util.EnumSet

class BulkTaskState(
    val taskId: String,
    val operationType: String,
    val pattern: String,
    val startTime: Long,
    val numSuccess: Int,
    val numFailed: Int,
    val numPending: Int,
    val numCancelled: Int,
    val failedIndices: List<FailedIndex>
) : Writeable {

    constructor(inp: StreamInput) : this(
        taskId = inp.readString(),
        operationType = inp.readString(),
        pattern = inp.readString(),
        startTime = inp.readLong(),
        numSuccess = inp.readInt(),
        numFailed = inp.readInt(),
        numPending = inp.readInt(),
        numCancelled = inp.readInt(),
        failedIndices = inp.readList { i -> FailedIndex(i.readString(), i.readString()) }
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(taskId)
        out.writeString(operationType)
        out.writeString(pattern)
        out.writeLong(startTime)
        out.writeInt(numSuccess)
        out.writeInt(numFailed)
        out.writeInt(numPending)
        out.writeInt(numCancelled)
        out.writeCollection(failedIndices) { o, fi -> o.writeString(fi.index); o.writeString(fi.reason) }
    }

}

class BulkReplicationTaskMetadata(val taskState: BulkTaskState?) : Metadata.Custom {

    companion object {
        const val NAME = "bulk_replication_task"
        val EMPTY = BulkReplicationTaskMetadata(null)
        // Task state is not restored on restart; the executing coroutine is terminated and partial state is unsafe to resume.
        fun fromXContent(parser: XContentParser): BulkReplicationTaskMetadata = EMPTY
    }

    constructor(inp: StreamInput) : this(if (inp.readBoolean()) BulkTaskState(inp) else null)

    override fun writeTo(out: StreamOutput) {
        if (taskState != null) { out.writeBoolean(true); taskState.writeTo(out) }
        else out.writeBoolean(false)
    }

    override fun diff(previousState: Metadata.Custom) = Diff(this)
    override fun getWriteableName() = NAME
    override fun getMinimalSupportedVersion(): Version = Version.V_2_0_0

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        if (taskState != null) {
            builder.field("task_id", taskState.taskId)
            builder.field("operation_type", taskState.operationType)
            builder.field("pattern", taskState.pattern)
            builder.field("start_time", taskState.startTime)
            builder.field("num_success", taskState.numSuccess)
            builder.field("num_failed", taskState.numFailed)
            builder.field("num_pending", taskState.numPending)
            builder.field("num_cancelled", taskState.numCancelled)
            builder.startArray("failed_indices")
            taskState.failedIndices.forEach { fi ->
                builder.startObject().field("index", fi.index).field("failure_reason", fi.reason).endObject()
            }
            builder.endArray()
        }
        return builder
    }

    override fun context(): EnumSet<Metadata.XContentContext> = EnumSet.of(Metadata.XContentContext.API)

    class Diff(private val current: BulkReplicationTaskMetadata) : NamedDiff<Metadata.Custom> {
        constructor(inp: StreamInput) : this(BulkReplicationTaskMetadata(inp))
        override fun writeTo(out: StreamOutput) = current.writeTo(out)
        override fun getWriteableName() = NAME
        override fun apply(part: Metadata.Custom): Metadata.Custom = current
    }
}
