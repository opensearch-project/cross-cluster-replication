package com.amazon.elasticsearch.replication

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.elasticsearch.common.logging.Loggers
import java.lang.IllegalStateException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

class TranslogBuffer(sizeBytes: Long) {
    // TODO: rename to 'empty'/'not_present'?
    val FIRST_FETCH = -1L

    val log = Loggers.getLogger(javaClass, "translogbuffer")!!

    var translogBufferMutex = Mutex()

    val bufferInitialSize = sizeBytes

    /** Variable translogBuffer captures the size of buffer which will hold the in-flight translog batches, which are
     * fetched from leader but yet to be applied to follower. All changes to translogBuffer must happen after acquiring
     * a lock on [translogBufferMutex]. */
    private val translogBuffer = AtomicLong(sizeBytes)

    /** We keep estimate of size of a translog batch in [batchSizeEstimate] map, so that we can use it as a guess of
     *  how much a to-be fetched batch is going to consume.
     *  Note that all mutating operations to this map should be done after acquiring lock on [translogBufferMutex].
     *  Key is index name, value is estimate size of one batch
     */
    private var batchSizeEstimate = ConcurrentHashMap<String, Long>()
    // TODO: add comment about negation
    private var indexInactive = ConcurrentHashMap<String, Boolean>()

    suspend fun getBatchSizeEstimateOrLockIfFirstFetch(followerIndexName: String): Long {
        log.info("getbatchsize locking")
        translogBufferMutex.lock()
        log.info("getbatchsize locked")
        if (!batchSizeEstimate.containsKey(followerIndexName)) {
            log.info("getbatchsize key not found")
            return FIRST_FETCH
        }
        log.info("getbatchsize key found")
        val retval = batchSizeEstimate[followerIndexName]!!
        translogBufferMutex.unlock()
        log.info("getbatchsize unlocked")
        return retval
    }

   fun addEstimateAndUnlock(followerIndexName: String, estimate: Long) {
       if (!translogBufferMutex.isLocked) {
           throw IllegalStateException("Translog buffer mutex should be locked but it isn't")
       }
       batchSizeEstimate[followerIndexName] = estimate
       translogBufferMutex.unlock()
   }

    fun unlockIfLocked() {
        // TODO: can there still be issue here? What if this is a second index for which replication is started on this
        //  node, and the lock is acquired by the first index, but there is an error in the first fetch of this second
        //  index. If that happens, we'll likely be accidentally unlocking this lock which was actually held by the first index
        if (translogBufferMutex.isLocked) {
            translogBufferMutex.unlock()
        }
    }

    // false, true
    // TODO: better name for previousIndexInactiveState -> indexInactiveWhenBatchAdded
    suspend fun removeBatch(followerIndexName: String, markIndexInactive: Boolean, previousIndexInactiveState: Boolean): Boolean {
        log.info("removebatch started")
        translogBufferMutex.withLock {
            log.info("removebatch took lock")
            log.info("buffer size is ${translogBuffer.get()}, estimate is ${batchSizeEstimate[followerIndexName]} and initial buffer size is $bufferInitialSize")
            indexInactive[followerIndexName] = markIndexInactive
            if (batchSizeEstimate.containsKey(followerIndexName) &&
                    translogBuffer.get() + batchSizeEstimate[followerIndexName]!! <= bufferInitialSize &&
                    !previousIndexInactiveState) {
                log.info("removebatch condition satisfied")
                translogBuffer.addAndGet(batchSizeEstimate[followerIndexName]!!)
                return true
            }
        }
        log.info("removebatch condition not satisfied")
        return false
    }

    // TODO add comment and info about inactive
    suspend fun addBatch(followerIndexName: String): Pair<Boolean, Boolean> {
        var isIndexInactive = false
        translogBufferMutex.withLock {
            if (indexInactive.containsKey(followerIndexName)) {
                isIndexInactive = indexInactive[followerIndexName]!!
            }
            if (batchSizeEstimate.containsKey(followerIndexName) &&
                    translogBuffer.get() > batchSizeEstimate[followerIndexName]!! &&
                    !isIndexInactive) {
                translogBuffer.addAndGet(-1 * batchSizeEstimate[followerIndexName]!!)
                return Pair(true, isIndexInactive)
            }
        }
        return Pair(false, isIndexInactive)
    }
}