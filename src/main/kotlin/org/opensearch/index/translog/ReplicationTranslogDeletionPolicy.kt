package org.opensearch.index.translog

import org.opensearch.index.IndexSettings
import org.opensearch.index.seqno.RetentionLease
import org.opensearch.index.seqno.RetentionLeases
import org.opensearch.replication.ReplicationPlugin
import java.io.IOException
import java.util.function.Supplier

class ReplicationTranslogDeletionPolicy(
    indexSettings: IndexSettings,
    private val retentionLeasesSupplier: Supplier<RetentionLeases>
) : TranslogDeletionPolicy(
    indexSettings.translogRetentionSize.bytes,
    indexSettings.translogRetentionAge.millis,
    indexSettings.translogRetentionTotalFiles
) {
    @Volatile
    private var translogPruningEnabled: Boolean =
        ReplicationPlugin.INDEX_PLUGINS_REPLICATION_TRANSLOG_RETENTION_LEASE_PRUNING_ENABLED_SETTING.get(indexSettings.settings)

    init {
        indexSettings.scopedSettings.addSettingsUpdateConsumer(
            ReplicationPlugin.INDEX_PLUGINS_REPLICATION_TRANSLOG_RETENTION_LEASE_PRUNING_ENABLED_SETTING
        ) { value: Boolean -> translogPruningEnabled = value }
    }

    /**
     * returns the minimum translog generation that is still required by the system. Any generation below
     * the returned value may be safely deleted
     *
     * @param readers current translog readers
     * @param writer  current translog writer
     */
    @Synchronized
    @Throws(IOException::class)
    override fun minTranslogGenRequired(readers: List<TranslogReader>, writer: TranslogWriter): Long {
        val minBySize: Long = getMinTranslogGenBySize(readers, writer, retentionSizeInBytes)
        var minByRetentionLeasesAndSize = Long.MAX_VALUE
        if (translogPruningEnabled) {
            // If retention size is specified, size takes precedence.
            val minByRetentionLeases: Long = getMinTranslogGenByRetentionLease(readers, writer)
            minByRetentionLeasesAndSize = minBySize.coerceAtLeast(minByRetentionLeases)
        }
        val minByAge = getMinTranslogGenByAge(readers, writer, retentionAgeInMillis, currentTime())
        val minByAgeAndSize = if (minBySize == Long.MIN_VALUE && minByAge == Long.MIN_VALUE) {
            // both size and age are disabled;
            Long.MAX_VALUE
        } else {
            minByAge.coerceAtLeast(minBySize)
        }
        val minByNumFiles = getMinTranslogGenByTotalFiles(readers, writer, retentionTotalFiles)
        val minByLocks: Long = minTranslogGenRequiredByLocks
        val minByTranslogGenSettings = minByAgeAndSize.coerceAtLeast(minByNumFiles).coerceAtMost(minByLocks)

        // If retention size is specified, size takes precedence.
        return minByTranslogGenSettings.coerceAtMost(minBySize.coerceAtLeast(minByRetentionLeasesAndSize))
    }

    private fun getMinTranslogGenByRetentionLease(readers: List<TranslogReader>, writer: TranslogWriter): Long {
        var minGen: Long = writer.getGeneration();
        val minimumRetainingSequenceNumber: Long = retentionLeasesSupplier.get()
            .leases()
            .stream()
            .mapToLong(RetentionLease::retainingSequenceNumber)
            .min()
            .orElse(Long.MAX_VALUE);

        for (i in readers.size - 1 downTo 0) {
            val reader: TranslogReader = readers[i]
            if (reader.minSeqNo <= minimumRetainingSequenceNumber &&
                reader.maxSeqNo >= minimumRetainingSequenceNumber
            ) {
                minGen = minGen.coerceAtMost(reader.getGeneration());
            }
        }
        return minGen;
    }
}