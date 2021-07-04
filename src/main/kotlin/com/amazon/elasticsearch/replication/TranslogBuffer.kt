package com.amazon.elasticsearch.replication

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.elasticsearch.common.logging.Loggers
import java.lang.IllegalStateException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/** [TranslogBuffer] captures the size of buffer which will hold the in-flight translog batches, which are
 * fetched from leader but yet to be applied to follower. All changes to translogBuffer must happen after acquiring a
 * lock on [mutex].
 * Also see [TranslogSequencer]. Note that TranslogSequencer might hold additional memory for out-of-order translogs,
 * and that memory consumption is not captured or capped by [TranslogBuffer]
 */
class TranslogBuffer(sizeBytes: Long) {
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

    /** Map to keep track of which indices are inactive. Calls to fetch translog for inactive indices doesn't consume
     * memory from buffer. But once translogs started arriving, the index should be marked active again. Using negation
     * ('inactive') here as we want to use Boolean default of 'false' for normal case.
     *
     */
    private var indexInactive = ConcurrentHashMap<String, Boolean>()

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
        // TODO: can there still be issue here? What if this is a second index for which replication is started on this
        //  node, and the lock is acquired by the first index, but there is an error in the first fetch of this second
        //  index. If that happens, we'll likely be accidentally unlocking this lock which was actually held by the first index
        if (mutex.isLocked) {
            mutex.unlock()
        }
    }

    /** Add batch to buffer. If the index is inactive, make it active again */
    suspend fun addBatch(followerIndexName: String): Pair<Boolean, Boolean> {
        var isIndexInactive = false
        mutex.withLock {
            if (indexInactive.containsKey(followerIndexName)) {
                isIndexInactive = indexInactive[followerIndexName]!!
            }
            if (isIndexInactive) {
                // No need to add to buffer if the index is inactive
                return Pair(true, isIndexInactive)
            }
            if (batchSizeEstimate.containsKey(followerIndexName) && buffer.get() > batchSizeEstimate[followerIndexName]!!) {
                val currSize = buffer.addAndGet(-1 * batchSizeEstimate[followerIndexName]!!)
                log.debug("${batchSizeEstimate[followerIndexName]!!} bytes added to buffer. Buffer is now $currSize bytes")
                return Pair(true, isIndexInactive)
            }
        }
        return Pair(false, isIndexInactive)
    }

    /** Method to remove batch from buffer. [inactiveWhenBatchAdded] tells whether the index was inactive at the time
     * when this batch was added to buffer.
     */
    suspend fun removeBatch(followerIndexName: String, markIndexInactive: Boolean, inactiveWhenBatchAdded: Boolean): Boolean {
        mutex.withLock {
            indexInactive[followerIndexName] = markIndexInactive
            if (inactiveWhenBatchAdded) {
                // No need to remove from buffer if the index was inactive when batch was added to buffer
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