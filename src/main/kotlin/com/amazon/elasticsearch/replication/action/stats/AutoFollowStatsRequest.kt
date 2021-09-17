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

import com.amazon.elasticsearch.replication.task.autofollow.AutoFollowTask
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.support.tasks.BaseTasksRequest
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.tasks.Task
import java.io.IOException

/**
 * A request to get  replication autofollow stats.
 */
class AutoFollowStatsRequest : BaseTasksRequest<AutoFollowStatsRequest> {

    constructor(inp: StreamInput) : super(inp)

    constructor():super()

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
    }

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    override fun match(task: Task?): Boolean {
        if (task is AutoFollowTask) {
            return true
        }
        return false
    }

}

