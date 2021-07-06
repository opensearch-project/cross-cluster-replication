package com.amazon.elasticsearch.replication

import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import org.elasticsearch.common.logging.Loggers
import java.lang.IllegalStateException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException
import org.elasticsearch.index.shard.ShardId

/** [TranslogBuffer] captures the size of buffer which will hold the in-flight translog batches, which are
 * fetched from leader but yet to be applied to follower. All changes to translogBuffer must happen after acquiring a
 * lock on [mutex].
 * Also see [TranslogSequencer]. Note that TranslogSequencer might hold additional memory for out-of-order translogs,
 * and that memory consumption is not captured or capped by [TranslogBuffer]
 */
class TranslogBuffer(val sizeBytes: Long, val fetchParallelism: Int) {

    val inFlightTranslogBytesLimit = sizeBytes
    private val inFlightTranslogBytes = AtomicLong(0)
    private val rateLimiter = Semaphore(fetchParallelism)
    private val shardToDelay = ConcurrentHashMap<String, Long>()

    fun markBatchAdded(bytes: Long): Releasable {
        val newBytes = inFlightTranslogBytes.addAndGet(bytes)
        if (newBytes > inFlightTranslogBytesLimit) {
            inFlightTranslogBytes.addAndGet(-bytes)
            throw EsRejectedExecutionException("blah")
        } else {
            return Releasable { inFlightTranslogBytes.addAndGet(-bytes) }
        }
    }

    suspend fun acquireRateLimiter(shardId: ShardId) {
        if (shardToDelay.containsKey(shardId.toString())) {
            delay(shardToDelay[shardId.toString()]!!)
        }
        rateLimiter.acquire()
    }

    suspend fun releaseRateLimiter(shardId: ShardId, operationSuccessful: Boolean = true) {
        if (operationSuccessful) {
            shardToDelay[shardId.toString()] = 0
            rateLimiter.release()
            return
        }
        if (!shardToDelay.containsKey(shardId.toString())) {
            shardToDelay[shardId.toString()] = 0
        }
        val maxDelay = 5 * 60_000L          // 5 minutes
        val delayIncrementStep = 15_000L     // 15 seconds
        shardToDelay[shardId.toString()] = shardToDelay[shardId.toString()]!! + delayIncrementStep
        if (shardToDelay[shardId.toString()]!! > maxDelay) {
            shardToDelay[shardId.toString()] = maxDelay
        }
        rateLimiter.acquire()
    }


    val FIRST_FETCH = -1L
    val log = Loggers.getLogger(javaClass, "translogbuffer")!!

    private var mutex = Mutex()
    val bufferInitialSize = sizeBytes

    /** Variable [buffer] captures the size of buffer which will hold the in-flight translog batches, which are
     * fetched from leader but yet to be applied to follower. All changes to [buffer] must happen after acquiring
     * a lock on [mutex]. */
    private val buffer = AtomicLong(sizeBytes)

    /** We keep estimate of size of a translog batch in [batchSizeEstimate] map, so that we can use it as a guess of
     *  how much a to-be fetched batch is going to consume.
     *  Note that all mutating operations to this map should be done after acquiring lock on [mutex].
     *  Key is index name, value is estimate size of one batch
     */
    private var batchSizeEstimate = ConcurrentHashMap<String, Long>()

    /** Map to keep track of which shards are inactive. Calls to fetch translog for inactive shards doesn't consume
     * memory from buffer. But once translogs started arriving, the shards should be marked active again. Using negation
     * ('inactive') here as we want to use Boolean default of 'false' for normal case.
     */
    private var shardInactive = ConcurrentHashMap<String, Boolean>()

    /**  Return batch size estimate for the provided index. If this is the first fetch, don't release the lock and
     * return [FIRST_FETCH] */
    suspend fun getBatchSizeEstimateOrLockIfFirstFetch(followerIndexName: String): Long {
        mutex.lock()
        if (!batchSizeEstimate.containsKey(followerIndexName)) {
            return FIRST_FETCH
        }
        val retval = batchSizeEstimate[followerIndexName]!!
        mutex.unlock()
        return retval
    }

   fun addEstimateAfterFirstFetchAndUnlock(followerIndexName: String, estimate: Long) {
       if (!mutex.isLocked) {
           throw IllegalStateException("Translog buffer mutex should be locked but it isn't")
       }
       batchSizeEstimate[followerIndexName] = estimate
       mutex.unlock()
   }

    fun unlockIfLocked() {
        if (mutex.isLocked) {
            mutex.unlock()
        }
    }

    /** Add batch to buffer. If the shard is inactive, make it active again */
    suspend fun addBatch(followerIndexName: String, shardName: String): Pair<Boolean, Boolean> {
        var isShardInactive = false
        mutex.withLock {
            if (shardInactive.containsKey(shardName)) {
                isShardInactive = shardInactive[shardName]!!
            }
            if (isShardInactive) {
                // No need to add to buffer if the shard is inactive
                return Pair(true, isShardInactive)
            }
            if (batchSizeEstimate.containsKey(followerIndexName) && buffer.get() > batchSizeEstimate[followerIndexName]!!) {
                val currSize = buffer.addAndGet(-1 * batchSizeEstimate[followerIndexName]!!)
                log.debug("${batchSizeEstimate[followerIndexName]!!} bytes added to buffer. Buffer is now $currSize bytes")
                return Pair(true, isShardInactive)
            }
        }
        return Pair(false, isShardInactive)
    }

    /** Method to remove batch from buffer. [inactiveWhenBatchAdded] tells whether the shard was inactive at the time
     * when this batch was added to buffer.
     */
    suspend fun removeBatch(followerIndexName: String, shardName: String, markShardInactive: Boolean, inactiveWhenBatchAdded: Boolean): Boolean {
        mutex.withLock {
            shardInactive[shardName] = markShardInactive
            if (inactiveWhenBatchAdded) {
                // No need to remove from buffer if the shard was inactive when batch was added to buffer
                return true
            }
            if (batchSizeEstimate.containsKey(followerIndexName) && buffer.get() + batchSizeEstimate[followerIndexName]!! <= bufferInitialSize) {
                val currSize = buffer.addAndGet(batchSizeEstimate[followerIndexName]!!)
                log.debug("${batchSizeEstimate[followerIndexName]!!} bytes removed from buffer. Buffer is now $currSize bytes")
                return true
            }
        }
        return false
    }
}