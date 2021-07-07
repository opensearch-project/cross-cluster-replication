package com.amazon.elasticsearch.replication

import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Semaphore
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.monitor.jvm.JvmInfo

class TranslogBuffer(val percentOfHeap: Int, val fetchParallelism: Int) {

    val SEMAPHORE_PERMITS_UPPER_CAP = 500
    val log = Loggers.getLogger(javaClass, "translogbuffer")!!
    private val inFlightTranslogBytesLimit = AtomicLong(JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * percentOfHeap/100)
    private val inFlightTranslogBytes = AtomicLong(0)
    private val rateLimiter = Semaphore(SEMAPHORE_PERMITS_UPPER_CAP, SEMAPHORE_PERMITS_UPPER_CAP-fetchParallelism)
    private val rateLimiter2 = Semaphore(SEMAPHORE_PERMITS_UPPER_CAP, SEMAPHORE_PERMITS_UPPER_CAP-fetchParallelism)
    /** update to parallelism will reflect here first, which will then later be applied to [parallelism] */
    private var parallelismNewVal = fetchParallelism
    private var parallelism = fetchParallelism
    private val shardToDelay = ConcurrentHashMap<String, AtomicLong>()

    // Only used in tests at the moment
    fun getParallelism(): Int {
        return parallelism
    }

    fun updateParallelism(value: Int) {
        parallelismNewVal = value
    }

    private suspend fun updateParallelismIfChanged() {
        if (parallelism == parallelismNewVal) {
            return
        }
        if (parallelismNewVal < 1) {
            parallelismNewVal = 1
        } else if (parallelismNewVal > SEMAPHORE_PERMITS_UPPER_CAP) {
            parallelismNewVal = SEMAPHORE_PERMITS_UPPER_CAP
        }

        if (parallelism > parallelismNewVal) {
            val diff = parallelism-parallelismNewVal
            for (i in 1..diff) {
                rateLimiter.acquire()
                rateLimiter2.acquire()
            }
            log.info("Parallelism decreased by $diff, was $parallelism")
        } else if (parallelism < parallelismNewVal) {
            val diff = parallelismNewVal-parallelism
            for (i in 1..diff) {
                rateLimiter.release()
                rateLimiter2.release()
            }
            log.info("Parallelism increased by $diff, was $parallelism")
        }
        parallelism = parallelismNewVal
    }

    fun updateBufferSize(percentOfHeap: Int) {
        val bytes = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * percentOfHeap/100
        // If the consumed buffer is over-capacity after update, then the consumed memory will only decrease till it
        // comes below the new updated value
        log.info("Updated buffer size to $bytes ($percentOfHeap % of heap)")
        inFlightTranslogBytesLimit.set(bytes)
    }

    suspend fun markBatchAdded(bytes: Long): Releasable {
        updateParallelismIfChanged()
        log.debug("adding $bytes bytes")
        val newBytes = inFlightTranslogBytes.addAndGet(bytes)
        if (newBytes > inFlightTranslogBytesLimit.get()) {
            inFlightTranslogBytes.addAndGet(-bytes)
            log.info("$bytes bytes addition failed")
            throw EsRejectedExecutionException("Translog buffer is full. Buffer capacity: " +
                    "${inFlightTranslogBytesLimit.get()}, current size ${inFlightTranslogBytes.get()}, request size $bytes")
        } else {
            log.debug("$bytes bytes addition succeeded")
            return Releasable {
                log.debug("$bytes bytes released now")
                inFlightTranslogBytes.addAndGet(-bytes)
            }
        }
    }

    suspend fun acquireRateLimiter(shardId: ShardId) {
        updateParallelismIfChanged()
        if (shardToDelay.containsKey(shardId.toString())) {
            val delayMs = shardToDelay[shardId.toString()]!!.get()
            if (delayMs > 0) {
                log.debug("delaying rate limiter acquisition by $delayMs ms")
                delay(delayMs)
            }
        }
        log.debug("acquiring ratelimiter for shard ${shardId.toString()}")
        rateLimiter.acquire()
    }

    fun releaseRateLimiter(shardId: ShardId, operationSuccessful: Boolean) {
        if (operationSuccessful) {
            shardToDelay[shardId.toString()] = AtomicLong(0)
            log.debug("Successful operation. Now releasing ratelimiter for shard ${shardId.toString()}")
            rateLimiter.release()
            return
        }
        if (!shardToDelay.containsKey(shardId.toString())) {
            shardToDelay[shardId.toString()] = AtomicLong(0)
        }
        val maxDelay = 60_000L              // 1 minute
        val delayIncrementStep = 10_000L    // 10 seconds
        log.debug("adding delay ${delayIncrementStep} ms to shard ${shardId}")
        shardToDelay[shardId.toString()]!!.getAndAdd(delayIncrementStep)

        if (shardToDelay[shardId.toString()]!!.get() > maxDelay) {
            shardToDelay[shardId.toString()]!!.set(maxDelay)
            log.debug("updating delay ${maxDelay} to shard ${shardId}")
        }
        log.debug("now releasing ratelimiter for shard ${shardId.toString()}")
        rateLimiter.release()
    }

    suspend fun acquireRateLimiter2() {
        updateParallelismIfChanged()
        log.debug("acquiring ratelimiter2")
        rateLimiter2.acquire()
    }

    fun releaseRateLimiter2() {
        log.debug("releasing ratelimiter2")
        rateLimiter2.release()
    }
}
