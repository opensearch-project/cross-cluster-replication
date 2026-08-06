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

package org.opensearch.replication

import org.opensearch.common.lucene.Lucene
import org.opensearch.index.VersionType
import org.opensearch.index.engine.Engine
import org.opensearch.index.engine.EngineTestCase
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.index.store.Store
import org.opensearch.index.translog.Translog
import java.nio.file.Path

class ReplicationEngineTests : EngineTestCase() {

    private lateinit var replicationEngine: ReplicationEngine
    private lateinit var replicationStore: Store
    private lateinit var replicationTranslogDir: Path

    override fun setUp() {
        super.setUp()
        replicationStore = createStore()
        Lucene.cleanLuceneIndex(replicationStore.directory())
        replicationTranslogDir = createTempDir("translog-replication")
        val config = config(engine.config(), replicationStore, replicationTranslogDir, engine.config().tombstoneDocSupplier)
        replicationStore.createEmpty(config.indexSettings.indexVersionCreated.luceneVersion)
        val translogUuid = Translog.createEmptyTranslog(
            replicationTranslogDir,
            SequenceNumbers.NO_OPS_PERFORMED,
            shardId,
            primaryTerm.get()
        )
        replicationStore.associateIndexWithNewTranslog(translogUuid)
        replicationEngine = ReplicationEngine(config)
        replicationEngine.translogManager().recoverFromTranslog(
            createTranslogHandler(config.indexSettings, replicationEngine),
            replicationEngine.processedLocalCheckpoint,
            Long.MAX_VALUE
        )
    }

    override fun tearDown() {
        replicationEngine.close()
        replicationStore.close()
        super.tearDown()
    }

    fun testFillSeqNoGapsIsNoOpEvenWithGap() {
        // Index doc at seqNo 0 (uses the ReplicationEngine's generateSeqNoForOperationOnPrimary
        // which returns the pre-assigned seqNo)
        val doc0 = createParsedDoc("0", null)
        val op0 = Engine.Index(
            newUid(doc0), doc0, 0L, primaryTerm.get(), 1L, VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1L, false,
            SequenceNumbers.UNASSIGNED_SEQ_NO, 0L
        )
        replicationEngine.index(op0)

        // Index doc at seqNo 2 (skipping seqNo 1 to create a gap)
        val doc2 = createParsedDoc("2", null)
        val op2 = Engine.Index(
            newUid(doc2), doc2, 2L, primaryTerm.get(), 1L, VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1L, false,
            SequenceNumbers.UNASSIGNED_SEQ_NO, 0L
        )
        replicationEngine.index(op2)

        // Verify the gap exists: local checkpoint should be 0 (seqNo 1 is missing)
        assertEquals(
            "Local checkpoint should be 0 because seqNo 1 is missing",
            0L, replicationEngine.processedLocalCheckpoint
        )

        // Call fillSeqNoGaps - on ReplicationEngine this MUST be a no-op
        val filled = replicationEngine.fillSeqNoGaps(primaryTerm.get())
        assertEquals("ReplicationEngine.fillSeqNoGaps() must return 0 (no-op)", 0, filled)

        // Verify the gap is still there (no no-ops were written to fill seqNo 1)
        assertEquals(
            "Local checkpoint should still be 0 - the gap must NOT be filled",
            0L, replicationEngine.processedLocalCheckpoint
        )
    }

    fun testBaselineInternalEngineFillsGaps() {
        // Create the same gap scenario on the standard engine
        val doc0 = createParsedDoc("baseline-0", null)
        engine.index(indexForDoc(doc0))

        // Generate a seqNo without indexing to create a gap
        generateNewSeqNo(engine)

        // Index another doc (takes the next seqNo after the gap)
        val doc2 = createParsedDoc("baseline-2", null)
        engine.index(indexForDoc(doc2))

        // The engine's local checkpoint should have a gap
        val lcpBefore = engine.processedLocalCheckpoint

        // fillSeqNoGaps on InternalEngine should fill the gap
        val filled = engine.fillSeqNoGaps(primaryTerm.get())
        assertTrue("InternalEngine.fillSeqNoGaps() should fill the gap (filled=$filled)", filled > 0)

        // After filling, the local checkpoint advances
        assertTrue(
            "Local checkpoint should advance after filling gaps",
            engine.processedLocalCheckpoint > lcpBefore
        )
    }
}
