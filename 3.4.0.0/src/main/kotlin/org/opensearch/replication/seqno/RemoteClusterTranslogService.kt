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

import org.apache.logging.log4j.LogManager
import org.opensearch.ResourceNotFoundException
import org.opensearch.common.lifecycle.AbstractLifecycleComponent
import org.opensearch.common.inject.Singleton
import org.opensearch.index.engine.Engine
import org.opensearch.index.shard.IndexShard
import org.opensearch.index.translog.Translog

@Singleton
class RemoteClusterTranslogService : AbstractLifecycleComponent(){
    companion object {
        private val log = LogManager.getLogger(RemoteClusterTranslogService::class.java)
        private const val SOURCE_NAME = "os_plugin_replication"
    }

    override fun doStart() {
    }

    override fun doStop() {
    }

    override fun doClose() {
    }
    
    public fun getHistoryOfOperations(indexShard: IndexShard, startSeqNo: Long, toSeqNo: Long): List<Translog.Operation> {
        log.trace("Fetching translog snapshot for $indexShard - from $startSeqNo to $toSeqNo")
        // Ref issue: https://github.com/opensearch-project/OpenSearch/issues/2482
        val snapshot = indexShard.getHistoryOperationsFromTranslog(startSeqNo, toSeqNo)

        // Total ops to be fetched (both toSeqNo and startSeqNo are inclusive)
        val opsSize = toSeqNo - startSeqNo + 1
        val ops = ArrayList<Translog.Operation>(opsSize.toInt())

        // Filter and sort specific ops from the obtained history
        var filteredOpsFromTranslog = 0
        snapshot.use {
            var op  = snapshot.next()
            while(op != null) {
                if(op.seqNo() in startSeqNo..toSeqNo) {
                    ops.add(op)
                    filteredOpsFromTranslog++
                }
                op = snapshot.next()
            }
        }
        assert(filteredOpsFromTranslog == opsSize.toInt()) {"Missing operations while fetching from translog"}

        val sortedOps = ArrayList<Translog.Operation>(opsSize.toInt())
        sortedOps.addAll(ops)
        for(ele in ops) {
            sortedOps[(ele.seqNo() - startSeqNo).toInt()] = ele
        }

        log.debug("Starting seqno after sorting ${sortedOps[0].seqNo()} and ending seqno ${sortedOps[ops.size-1].seqNo()}")
        return sortedOps.subList(0, ops.size.coerceAtMost((opsSize).toInt()))
    }
}
