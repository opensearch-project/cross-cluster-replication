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

package com.amazon.elasticsearch.replication.action.stats

import com.amazon.elasticsearch.replication.task.autofollow.AutoFollowStat
import org.elasticsearch.action.ActionType
import org.elasticsearch.action.FailedNodeException
import org.elasticsearch.action.TaskOperationFailure
import org.elasticsearch.action.support.tasks.BaseTasksResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import java.io.IOException

class AutoFollowStatsAction : ActionType<AutoFollowStatsResponses>(NAME, reader) {
    companion object {
        const val NAME = "indices:admin/plugins/replication/autofollow/stats"
        val INSTANCE = AutoFollowStatsAction()
        val reader = Writeable.Reader { inp -> AutoFollowStatsResponses(inp) }
    }

    override fun getResponseReader(): Writeable.Reader<AutoFollowStatsResponses> = reader
}


class AutoFollowStatsResponse : Writeable , ToXContentObject {
    val stat: AutoFollowStat

    constructor(inp: StreamInput) {
        stat = AutoFollowStat(inp)
    }

    constructor(status: AutoFollowStat) {
        this.stat = status
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        stat.writeTo(out)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return stat.toXContent(builder, params)
    }
}


class AutoFollowStatsResponses : BaseTasksResponse, ToXContentObject {
    val statsResponses: List<AutoFollowStatsResponse>
    var aggResponse = AutoFollowStat("", "")

    constructor(
            autoFollowStatsResponse: List<AutoFollowStatsResponse>,
            nodeFailures: List<FailedNodeException>,
            taskFailures: List<TaskOperationFailure>) : super(taskFailures, nodeFailures) {
        statsResponses = autoFollowStatsResponse
        for (resp in statsResponses) {
            aggResponse.failedLeaderCall += resp.stat.failedLeaderCall
            aggResponse.failCount += resp.stat.failCount
            aggResponse.failedIndices.addAll(resp.stat.failedIndices)
            aggResponse.successCount += resp.stat.successCount
        }
    }

    constructor(inp: StreamInput) : super(inp) {
        aggResponse = AutoFollowStat(inp)
        statsResponses = inp.readList { AutoFollowStatsResponse(inp) }
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        aggResponse.writeTo(out)
        out.writeList(statsResponses)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
        builder.startObject()
        builder.field("num_success_start_replication", aggResponse.successCount)
        builder.field("num_failed_start_replication", aggResponse.failCount)
        builder.field("num_failed_leader_calls", aggResponse.failedLeaderCall)
        builder.field("failed_indices", aggResponse.failedIndices)
        builder.startArray("autofollow_stats");
        for (response in statsResponses) {
            response.toXContent(builder, EMPTY_PARAMS)
        }
        builder.endArray(   )
        builder.endObject()
        return builder
    }

}


