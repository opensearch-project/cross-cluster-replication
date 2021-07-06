package com.amazon.elasticsearch.replication

import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Semaphore
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException
import org.elasticsearch.index.shard.ShardId

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
}
