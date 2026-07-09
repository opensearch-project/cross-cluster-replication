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

package org.opensearch.replication.seqno

import org.opensearch.common.lifecycle.AbstractLifecycleComponent
import org.opensearch.common.inject.Singleton
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentFragment
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.index.shard.ShardId
import java.util.concurrent.atomic.AtomicLong

class RemoteShardMetric  {
    var ops :AtomicLong = AtomicLong()
    var tlogSize :AtomicLong = AtomicLong()
    var opsLucene :AtomicLong = AtomicLong()
    var opsTlog :AtomicLong = AtomicLong()
    var latencyLucene :AtomicLong = AtomicLong()
    var latencyTlog :AtomicLong = AtomicLong()
    var bytesRead :AtomicLong = AtomicLong()
    var lastFetchTime :AtomicLong = AtomicLong()

    constructor()

    /**
     * Creates a serializable representation for these metrics.
     */
    fun createStats() : RemoteStats {
        return RemoteStats(ops.get(), tlogSize.get(), opsLucene.get(), opsTlog.get(),
                latencyLucene.get(), latencyTlog.get(), bytesRead.get(), lastFetchTime.get())
    }

     open class RemoteStats(ops :Long=0, tlogSize :Long=0, opsLucene :Long=0, opsTlog :Long=0, latencyLucene :Long=0, latencyTlog :Long=0,
                       bytesRead :Long=0, lastFetchTime :Long=0) : ToXContentFragment {

        var ops = ops
        var tlogSize = tlogSize
        var opsLucene = opsLucene
        var opsTlog = opsTlog
        var latencyLucene = latencyLucene
        var latencyTlog = latencyTlog
        var bytesRead = bytesRead
        var lastFetchTime = lastFetchTime

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
            builder.startObject()
            toXContentFragment(builder, params)
            return builder.endObject()
        }

         @Suppress("UNUSED_PARAMETER")
         fun toXContentFragment(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
             builder.field("operations_read", ops)
             builder.field("translog_size_bytes", tlogSize)
             builder.field("operations_read_lucene", opsLucene)
             builder.field("operations_read_translog", opsTlog)
             builder.field("total_read_time_lucene_millis", latencyLucene)
             builder.field("total_read_time_translog_millis", latencyTlog)
             builder.field("bytes_read", bytesRead)
             return builder
         }

        constructor(inp: StreamInput) : this()  {
            ops = inp.readLong()
            tlogSize = inp.readLong()
            opsLucene = inp.readLong()
            opsTlog = inp.readLong()
            latencyLucene = inp.readLong()
            latencyTlog = inp.readLong()
            bytesRead = inp.readLong()
            lastFetchTime = inp.readLong()
        }

        fun writeTo(out: StreamOutput) {
            out.writeLong(ops)
            out.writeLong(tlogSize)
            out.writeLong(opsLucene)
            out.writeLong(opsTlog)
            out.writeLong(latencyLucene)
            out.writeLong(latencyTlog)
            out.writeLong(bytesRead)
            out.writeLong(lastFetchTime)
        }

        fun add(stat :RemoteStats): RemoteStats {
            var newStat = this
            newStat.ops += stat.ops
            newStat.tlogSize += stat.tlogSize
            newStat.opsLucene += stat.opsLucene
            newStat.opsTlog += stat.opsTlog
            newStat.latencyLucene += stat.latencyLucene
            newStat.latencyTlog += stat.latencyTlog
            newStat.bytesRead += stat.bytesRead
            newStat.lastFetchTime = maxOf(lastFetchTime, stat.lastFetchTime)

            return newStat
        }
    }

    class RemoteStatsFrag() : RemoteShardMetric.RemoteStats(), ToXContentFragment {
        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
            return toXContentFragment(builder, params)
        }
    }

}

@Singleton
class RemoteClusterStats : AbstractLifecycleComponent(){
    var stats :MutableMap<ShardId, RemoteShardMetric> =  mutableMapOf()

    override fun doStart() {
    }

    override fun doStop() {
    }

    override fun doClose() {
    }

}
