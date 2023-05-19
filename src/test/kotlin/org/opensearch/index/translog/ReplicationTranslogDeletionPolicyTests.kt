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
package org.opensearch.index.translog

import org.apache.lucene.store.ByteArrayDataOutput
import org.hamcrest.Matchers.equalTo
import org.mockito.Mockito
import org.opensearch.common.UUIDs
import org.opensearch.common.bytes.BytesArray
import org.opensearch.common.bytes.ReleasableBytesReference
import org.opensearch.common.collect.Tuple
import org.opensearch.common.util.BigArrays
import org.opensearch.core.internal.io.IOUtils
import org.opensearch.index.seqno.RetentionLease
import org.opensearch.index.seqno.RetentionLeases
import org.opensearch.index.shard.ShardId
import org.opensearch.test.OpenSearchTestCase
import java.io.IOException
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.OpenOption
import java.nio.file.Path
import java.util.*
import java.util.function.Supplier


class ReplicationTranslogDeletionPolicyTests : OpenSearchTestCase() {
    private val TOTAL_OPS_IN_GEN = 10L

    @Throws(IOException::class)
    fun testWithRetentionLease() {
        val now = System.currentTimeMillis()
        val readersAndWriter: Tuple<MutableList<TranslogReader>, TranslogWriter?> = createReadersAndWriter(now)
        val allGens: MutableList<BaseTranslogReader> = ArrayList(readersAndWriter.v1())
        readersAndWriter.v2()?.let { allGens.add(it) }
        val retentionLeasesSupplier: Supplier<RetentionLeases> =
            createRetentionLeases(now, 0L, allGens.size * TOTAL_OPS_IN_GEN - 1)
        try {
            val minimumRetainingSequenceNumber: Long = retentionLeasesSupplier.get()
                .leases()
                .stream()
                .mapToLong { obj: RetentionLease -> obj.retainingSequenceNumber() }
                .min()
                .orElse(Long.MAX_VALUE)
            val selectedReader: Long = minimumRetainingSequenceNumber / TOTAL_OPS_IN_GEN
            val selectedGen = allGens[selectedReader.toInt()].generation
            assertThat(
                readersAndWriter.v2()?.let {
                    ReplicationTranslogDeletionPolicy.getMinTranslogGenByRetentionLease(
                        readersAndWriter.v1(),
                        it,
                        retentionLeasesSupplier
                    )
                },
                equalTo(selectedGen)
            )
        } finally {
            IOUtils.close(readersAndWriter.v1())
            IOUtils.close(readersAndWriter.v2())
        }
    }

    @Throws(Exception::class)
    fun testBySizeAndRetentionLease() {
        val now = System.currentTimeMillis()
        val readersAndWriter: Tuple<MutableList<TranslogReader>, TranslogWriter?> = createReadersAndWriter(now)
        val allGens: MutableList<BaseTranslogReader> = ArrayList(readersAndWriter.v1())
        readersAndWriter.v2()?.let { allGens.add(it) }
        try {
            val selectedReader = randomIntBetween(0, allGens.size - 1)
            val selectedGeneration = allGens[selectedReader].generation
            // Retaining seqno is part of lower gen
            val size =
                allGens.stream().skip(selectedReader.toLong()).map { obj: BaseTranslogReader -> obj.sizeInBytes() }
                    .reduce { a: Long, b: Long -> java.lang.Long.sum(a, b) }.get()
            var retentionLeasesSupplier: Supplier<RetentionLeases> =
                createRetentionLeases(now, 0L, selectedGeneration * TOTAL_OPS_IN_GEN - 1)
            assertThat(
                readersAndWriter.v2()?.let {
                    ReplicationTranslogDeletionPolicy.minTranslogGenRequired(
                        readersAndWriter.v1(), it,
                        true,
                        size,
                        Int.MAX_VALUE.toLong(),
                        Int.MAX_VALUE,
                        Int.MAX_VALUE.toLong(),
                        retentionLeasesSupplier
                    )
                },
                equalTo(selectedGeneration)
            )
            assertThat(
                TranslogDeletionPolicy.getMinTranslogGenByAge(
                    readersAndWriter.v1(),
                    readersAndWriter.v2(),
                    100L,
                    System.currentTimeMillis()
                ),
                equalTo(readersAndWriter.v2()?.generation)
            )

            // Retention lease is part of higher gen
            retentionLeasesSupplier = createRetentionLeases(
                now,
                selectedGeneration * TOTAL_OPS_IN_GEN,
                allGens.size * TOTAL_OPS_IN_GEN + TOTAL_OPS_IN_GEN - 1
            )
            assertThat(
                readersAndWriter.v2()?.let {
                    ReplicationTranslogDeletionPolicy.minTranslogGenRequired(
                        readersAndWriter.v1(), it,
                        true,
                        size,
                        Long.MIN_VALUE,
                        Int.MAX_VALUE,
                        Long.MAX_VALUE,
                        retentionLeasesSupplier
                    )
                },
                equalTo(selectedGeneration)
            )
        } finally {
            IOUtils.close(readersAndWriter.v1())
            IOUtils.close(readersAndWriter.v2())
        }
    }

    @Throws(IOException::class)
    private fun createRetentionLeases(now: Long, lowestSeqNo: Long, highestSeqNo: Long): Supplier<RetentionLeases> {
        val leases = LinkedList<RetentionLease>()
        val numberOfLeases = randomIntBetween(1, 5)
        for (i in 0 until numberOfLeases) {
            val seqNo = randomLongBetween(lowestSeqNo, highestSeqNo)
            leases.add(RetentionLease("test_$i", seqNo, now - (numberOfLeases - i) * 1000, "test"))
        }
        return Supplier { RetentionLeases(1L, 1L, leases) }
    }

    @Throws(IOException::class)
    private fun createReadersAndWriter(now: Long): Tuple<MutableList<TranslogReader>, TranslogWriter?> {
        val tempDir: Path = createTempDir()
        Files.createFile(tempDir.resolve(Translog.CHECKPOINT_FILE_NAME))
        var writer: TranslogWriter? = null
        val readers: MutableList<TranslogReader> = ArrayList()
        val numberOfReaders = randomIntBetween(0, 10)
        val translogUUID = UUIDs.randomBase64UUID(random())
        for (gen in 1..numberOfReaders + 1) {
            if (writer != null) {
                val reader = Mockito.spy(writer.closeIntoReader())
                Mockito.doReturn(writer.lastModifiedTime).`when`(reader).lastModifiedTime
                readers.add(reader)
            }
            writer = TranslogWriter.create(
                ShardId("index", "uuid", 0),
                translogUUID,
                gen.toLong(),
                tempDir.resolve(Translog.getFilename(gen.toLong())),
                { path: Path, options: Array<OpenOption> ->
                    FileChannel.open(
                        path,
                        *options
                    )
                },
                TranslogConfig.DEFAULT_BUFFER_SIZE,
                1L,
                1L,
                { 1L },
                { 1L },
                randomNonNegativeLong(),
                TragicExceptionHolder(),
                { },
                BigArrays.NON_RECYCLING_INSTANCE,
                false
            )
            writer = Mockito.spy(writer)
            Mockito.doReturn(now - (numberOfReaders - gen + 1) * 1000).`when`(writer).lastModifiedTime
            val bytes = ByteArray(4)
            val out = ByteArrayDataOutput(bytes)
            val startSeqNo: Long = (gen - 1) * TOTAL_OPS_IN_GEN
            val endSeqNo: Long = startSeqNo + TOTAL_OPS_IN_GEN - 1
            for (ops in endSeqNo downTo startSeqNo) {
                out.reset(bytes)
                out.writeInt(ops.toInt())
                writer.add(ReleasableBytesReference.wrap(BytesArray(bytes)), ops)
            }
        }
        return Tuple(readers, writer)
    }
}
