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
import kotlinx.coroutines.test.runBlockingTest

@ObsoleteCoroutinesApi
class TranslogBufferTests : ESTestCase() {

    @ExperimentalCoroutinesApi
    fun `test translog buffer`() = runBlockingTest {
        val tlb = TranslogBuffer(100)

        // perform first fetch and finish the first fetch
        val estimate = tlb.getBatchSizeEstimateOrLockIfFirstFetch("idx1")
        assert(estimate == tlb.FIRST_FETCH)
        tlb.addEstimateAfterFirstFetchAndUnlock("idx1", 30)

        // perform next 'normal' fetch by occupying buffer
        var (ok, inactive) = tlb.addBatch("idx1", "s1")
        assertEquals(true, ok)
        assertEquals(false, inactive)

        // try to occupy more memory than what the buffer can hold
        // Doesn't look like there's a way to destructure to same variables :(
        val (ok2, inactive2) = tlb.addBatch("idx1", "s1")
        assertEquals(true, ok2)
        assertEquals(false, inactive2)
        val (ok3, inactive3) = tlb.addBatch("idx1", "s1")
        assertEquals(true, ok3)
        assertEquals(false, inactive3)
        val (ok4, inactive4) = tlb.addBatch("idx1", "s1")
        assertEquals(false, ok4)
        assertEquals(false, inactive4)

        // replenishing buffer back again should succeed for 3 batches, and post that should fail
        ok = tlb.removeBatch("idx1", "s1", false, false)
        assertEquals(true, ok)
        ok = tlb.removeBatch("idx1", "s1", false, false)
        assertEquals(true, ok)
        ok = tlb.removeBatch("idx1", "s1", false, false)
        assertEquals(true, ok)
        ok = tlb.removeBatch("idx1", "s1", false, false)
        assertEquals(false, ok)

        // trying to remove when buffer is completely empty should fail
        ok = tlb.removeBatch("idx1", "s1", false, false)
        assertEquals(false, ok)

        // trying to remove when buffer is completely empty should fail, even if we're making index to become
        // inactive at the moment
        ok = tlb.removeBatch("idx1", "s1", true, false)
        assertEquals(false, ok)

        // marking an index inactive should allow for removing batches without memory constraint,
        // now that the previous state was also inactive when we added this batch
        ok = tlb.removeBatch("idx1", "s1", true, true)
        assertEquals(true, ok)

        // Adding batches to inactive index should work without memory constraint
        tlb.addBatch("idx1", "s1")
        tlb.addBatch("idx1", "s1")
        tlb.addBatch("idx1", "s1")
        tlb.addBatch("idx1", "s1")
        tlb.addBatch("idx1", "s1")
        val (ok5, inactive5) = tlb.addBatch("idx1", "s1")
        assertEquals(true, ok5)
        assertEquals(true, inactive5)
    }
}
