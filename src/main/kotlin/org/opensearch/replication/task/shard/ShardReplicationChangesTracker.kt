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

import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.opensearch.common.logging.Loggers
import org.opensearch.index.shard.IndexShard
import org.opensearch.replication.ReplicationSettings
import java.util.Collections
import java.util.concurrent.atomic.AtomicLong
import kotlin.collections.ArrayList

/**
 * Since we have added support for fetching batch of operations in parallel, we need to keep track of
 * how many operations have been fetched and what batch needs to be fetched next. This creates the
 * problem of concurrency with shared mutable state (https://kotlinlang.org/docs/shared-mutable-state-and-concurrency.html).
 * ShardReplicationChangesTracker abstracts away all that complexity from ShardReplicationTask.
 * Every reader coroutine in a shard has to interact with the tracker for:
 * 1. Requesting the range of operations to be fetched in the batch.
 * 2. Updating the final status of the batch fetch.
 */
class ShardReplicationChangesTracker(
    indexShard: IndexShard,
    private val replicationSettings: ReplicationSettings,
) {
    private val log = Loggers.getLogger(javaClass, indexShard.shardId())!!

    private val mutex = Mutex()
    private val missingBatches = Collections.synchronizedList(ArrayList<Pair<Long, Long>>())
    private val observedSeqNoAtLeader = AtomicLong(indexShard.localCheckpoint)
    private val seqNoAlreadyRequested = AtomicLong(indexShard.localCheckpoint)
    private val batchSizeSettings = BatchSizeSettings(indexShard.indexSettings(), replicationSettings)

    /**
     * Provides a range of operations to be fetched next.
     *
     * Here are the guarantees that this method provides:
     * 1. All reader coroutines get unique range of operations to fetch.
     * 2. It'll ensure that the complete range of operations would be fetched.
     * 3. Mutex in this method ensures that only one coroutine is requesting the batch at a time.
     *    If there are multiple coroutines, they'll be waiting in order to get the range of operations to fetch.
     * 4. If we've already fetched all the operations from leader, there would be one and only one
     *    reader polling on leader per shard.
     */
    suspend fun requestBatchToFetch(): Pair<Long, Long> {
        mutex.withLock {
            logDebug("Waiting to get batch. requested: ${seqNoAlreadyRequested.get()}, leader: ${observedSeqNoAtLeader.get()}")

            // Wait till we have batch to fetch. Note that if seqNoAlreadyRequested is equal to observedSeqNoAtLeader,
            // we still should be sending one more request to fetch which will just do a poll and eventually timeout
            // if no new operations are there on the leader (configured via TransportGetChangesAction.WAIT_FOR_NEW_OPS_TIMEOUT)
            while (seqNoAlreadyRequested.get() > observedSeqNoAtLeader.get() && missingBatches.isEmpty()) {
                delay(replicationSettings.pollDuration.millis)
            }

            // missing batch takes higher priority.
            return if (missingBatches.isNotEmpty()) {
                logDebug("Fetching missing batch ${missingBatches[0].first}-${missingBatches[0].second}")
                missingBatches.removeAt(0)
            } else {
                // return the next batch to fetch and update seqNoAlreadyRequested.
                val currentBatchSize = batchSizeSettings.getEffectiveBatchSize()
                val fromSeq = seqNoAlreadyRequested.getAndAdd(currentBatchSize.toLong()) + 1
                val toSeq = fromSeq + currentBatchSize - 1
                val sizeInfo =
                    if (batchSizeSettings.isDynamicallyReduced()) {
                        "reduced to $currentBatchSize"
                    } else {
                        "$currentBatchSize from ${batchSizeSettings.getBatchSizeSource()}"
                    }
                logDebug("Fetching the batch $fromSeq-$toSeq (batch size: $sizeInfo)")
                Pair(fromSeq, toSeq)
            }
        }
    }

    /**
     * Reduce batch size
     */
    fun reduceBatchSize() {
        batchSizeSettings.reduceBatchSize()
        logInfo("Batch size reduced to ${batchSizeSettings.getEffectiveBatchSize()}")
    }

    /**
     * Reset batch size
     */
    fun resetBatchSize() {
        batchSizeSettings.resetBatchSize()
        logInfo("Batch size reset to ${batchSizeSettings.getEffectiveBatchSize()}")
    }

    /**
     * Batch Size Settings
     */
    fun batchSizeSettings(): BatchSizeSettings = batchSizeSettings

    /**
     * Ensures that we've successfully fetched a particular range of operations.
     * In case of any failure(or we didn't get complete batch), we make sure that we're fetching the
     * missing operations in the next batch.
     */
    fun updateBatchFetched(
        success: Boolean,
        fromSeqNoRequested: Long,
        toSeqNoRequested: Long,
        toSeqNoReceived: Long,
        seqNoAtLeader: Long,
    ) {
        if (success) {
            // we shouldn't ever be getting more operations than requested.
            assert(
                toSeqNoRequested >= toSeqNoReceived,
            ) { "${Thread.currentThread().getName()} Got more operations in the batch than requested" }
            logDebug("Updating the batch fetched. $fromSeqNoRequested-$toSeqNoReceived/$toSeqNoRequested, seqNoAtLeader:$seqNoAtLeader")

            // If we didn't get the complete batch that we had requested.
            if (toSeqNoRequested > toSeqNoReceived) {
                // If this is the last batch being fetched, update the seqNoAlreadyRequested.
                if (seqNoAlreadyRequested.get() == toSeqNoRequested) {
                    seqNoAlreadyRequested.updateAndGet { toSeqNoReceived }
                } else {
                    // Else, add to the missing operations to missing batch
                    logDebug("Didn't get the complete batch. Adding the missing operations ${toSeqNoReceived + 1}-$toSeqNoRequested")
                    missingBatches.add(Pair(toSeqNoReceived + 1, toSeqNoRequested))
                }
            }

            // Update the sequence number observed at leader.
            observedSeqNoAtLeader.getAndUpdate { value -> if (seqNoAtLeader > value) seqNoAtLeader else value }
            logDebug("observedSeqNoAtLeader: ${observedSeqNoAtLeader.get()}")
        } else {
            // If this is the last batch being fetched, update the seqNoAlreadyRequested.
            if (seqNoAlreadyRequested.get() == toSeqNoRequested) {
                seqNoAlreadyRequested.updateAndGet { fromSeqNoRequested - 1 }
            } else {
                // If this was not the last batch, we might have already fetched other batch of
                // operations after this. Adding this to missing.
                logDebug("Adding batch to missing $fromSeqNoRequested-$toSeqNoRequested")
                missingBatches.add(Pair(fromSeqNoRequested, toSeqNoRequested))
            }
        }
    }

    private fun logDebug(msg: String) {
        log.debug("${Thread.currentThread().name}: $msg")
    }

    private fun logInfo(msg: String) {
        log.info("${Thread.currentThread().name}: $msg")
    }
}
