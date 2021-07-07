/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.elasticsearch.replication

import org.elasticsearch.test.ESTestCase
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runBlockingTest
import org.elasticsearch.monitor.jvm.JvmInfo
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.elasticsearch.common.lease.Releasable
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException
import org.elasticsearch.index.shard.ShardId

@ObsoleteCoroutinesApi
class TranslogBufferTests : ESTestCase() {

    @ExperimentalCoroutinesApi
    fun `test translog buffer`() = runBlockingTest {
        val tlb = TranslogBuffer(30, 5)
        val heapSize = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes()

        // buffer filled less than heap size should work
        var closeable = tlb.markBatchAdded(heapSize*20/100)
        closeable.close()

        // buffer filled over heap size should fail
        assertThatThrownBy {
            runBlockingTest {
                closeable = tlb.markBatchAdded(heapSize*40/100)
            }
        }.isInstanceOf(EsRejectedExecutionException::class.java)
                .hasMessageContaining("Translog buffer is full")

        // test if acquiring permit = parallelism works
        val parallelism = tlb.getParallelism()
        val tempShard = ShardId("testIndex", "testUUID", 1234)
        for (i in 1..parallelism) {
            tlb.acquireRateLimiter(tempShard)
        }
        for (i in 1..parallelism) {
            tlb.releaseRateLimiter(tempShard, true)
        }

        // test increasing parallelism
        var newParallelism = parallelism+2
        tlb.updateParallelism(newParallelism)
        val closeables: ArrayList<Releasable> = arrayListOf()
        for (i in 1..newParallelism) {
            tlb.acquireRateLimiter(tempShard)
            closeables.add( tlb.markBatchAdded(1))
        }
        for (i in 1..newParallelism) {
            closeables.removeAt(0).close()
            tlb.releaseRateLimiter(tempShard, true)
        }

        // test decreasing parallelism
        newParallelism = newParallelism-3
        tlb.updateParallelism(newParallelism)
        for (i in 1..newParallelism) {
            tlb.acquireRateLimiter(tempShard)
            closeables.add( tlb.markBatchAdded(1))
        }
        for (i in 1..newParallelism) {
            closeables.removeAt(0).close()
            tlb.releaseRateLimiter(tempShard, true)
        }
    }
}
