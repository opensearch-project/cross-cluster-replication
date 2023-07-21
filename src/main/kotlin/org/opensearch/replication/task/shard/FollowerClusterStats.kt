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

package org.opensearch.replication.task.shard

import org.apache.logging.log4j.LogManager
import org.opensearch.common.inject.Singleton
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentFragment
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.index.shard.ShardId
import java.util.concurrent.atomic.AtomicLong

class FollowerShardMetric  {
    var followerCheckpoint: Long = 0L
    var leaderCheckpoint: Long = 0L
    var opsWritten :AtomicLong = AtomicLong()
    var opsWriteFailures :AtomicLong = AtomicLong()
    var opsWriteThrottles :AtomicLong = AtomicLong()
    var opsRead :AtomicLong = AtomicLong()
    var opsReadFailures :AtomicLong = AtomicLong()
    var opsReadThrottles :AtomicLong = AtomicLong()
    val totalWriteTime :AtomicLong = AtomicLong()

    constructor()

    companion object {
        private val log = LogManager.getLogger(FollowerShardMetric::class.java)
    }

    /**
     * Creates a serializable representation for these metrics.
     */
    fun createStats() : FollowerStats {
        return FollowerStats(opsWritten.get(), opsWriteFailures.get(), opsWriteThrottles.get(), opsRead.get(), opsReadFailures.get(), opsReadThrottles.get(),
                followerCheckpoint, leaderCheckpoint, totalWriteTime.get())
    }

    // this can represent stats for an index as well as for a shard
    open class FollowerStats(var opsWritten: Long=0, var opsWriteFailures: Long=0, var opsWriteThrottles: Long=0, var opsRead: Long=0, var opsReadFailures: Long=0, var opsReadThrottles : Long=0,
                             var followerCheckpoint: Long=0, var leaderCheckpoint: Long=0, var totalWriteTime: Long=0) : ToXContentFragment {

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
            builder.startObject()
            toXContentFragment(builder, params)
            return builder.endObject()
        }

        @Suppress("UNUSED_PARAMETER")
        fun toXContentFragment(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
            builder.field("operations_written", opsWritten)
            builder.field("operations_read", opsRead)
            builder.field("failed_read_requests", opsReadFailures)
            builder.field("throttled_read_requests", opsReadThrottles)
            builder.field("failed_write_requests", opsWriteFailures)
            builder.field("throttled_write_requests", opsWriteThrottles)
            builder.field("follower_checkpoint", followerCheckpoint)
            builder.field("leader_checkpoint", leaderCheckpoint)
            builder.field("total_write_time_millis", totalWriteTime)
            return builder
        }

        constructor(inp: StreamInput) : this()  {
            opsWritten = inp.readLong()
            opsWriteFailures = inp.readLong()
            opsRead = inp.readLong()
            opsReadFailures = inp.readLong()
            opsReadThrottles = inp.readLong()
            followerCheckpoint = inp.readLong()
            leaderCheckpoint = inp.readLong()
            totalWriteTime = inp.readLong()
        }

        fun writeTo(out: StreamOutput) {
            out.writeLong(opsWritten)
            out.writeLong(opsWriteFailures)
            out.writeLong(opsRead)
            out.writeLong(opsReadFailures)
            out.writeLong(opsReadThrottles)
            out.writeLong(followerCheckpoint)
            out.writeLong(leaderCheckpoint)
            out.writeLong(totalWriteTime)
        }

        fun add(stat: FollowerStats) {
            opsWritten += stat.opsWritten
            opsWriteFailures += stat.opsWriteFailures
            opsWriteThrottles += stat.opsWriteThrottles
            opsRead += stat.opsRead
            opsReadFailures += stat.opsReadFailures
            opsReadThrottles += stat.opsReadThrottles
            followerCheckpoint += stat.followerCheckpoint
            leaderCheckpoint += stat.leaderCheckpoint
            totalWriteTime += stat.totalWriteTime
        }
    }

    // used only for cluster aggregation
    class FollowerStatsFragment(): FollowerStats(), ToXContentFragment {
        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
            return toXContentFragment(builder, params)
        }
    }
}

@Singleton
class FollowerClusterStats {
    var stats :MutableMap<ShardId, FollowerShardMetric> =  mutableMapOf()
}