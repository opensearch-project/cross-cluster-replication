package org.opensearch.index.translog

import org.opensearch.common.unit.ByteSizeValue
import org.opensearch.index.IndexSettings
import org.opensearch.index.seqno.RetentionLease
import org.opensearch.index.seqno.RetentionLeases
import org.opensearch.replication.ReplicationPlugin
import java.io.IOException
import java.util.function.Supplier

class ReplicationTranslogDeletionPolicy(
    indexSettings: IndexSettings,
    private val retentionLeasesSupplier: Supplier<RetentionLeases>
) : TranslogDeletionPolicy() {
    @Volatile
    private var translogPruningEnabled: Boolean =
        ReplicationPlugin.REPLICATION_INDEX_TRANSLOG_PRUNING_ENABLED_SETTING.get(indexSettings.settings)

    @Volatile
    private var retentionSizeInBytes: Long =
        if (translogPruningEnabled)
            ReplicationPlugin.REPLICATION_INDEX_TRANSLOG_RETENTION_SIZE.get(indexSettings.settings).bytes
        else indexSettings.translogRetentionSize.bytes


    @Volatile
    private var retentionAgeInMillis: Long = indexSettings.translogRetentionAge.millis

    @Volatile
    private var retentionTotalFiles: Int = indexSettings.translogRetentionTotalFiles

    init {
        indexSettings.scopedSettings.addSettingsUpdateConsumer(
            ReplicationPlugin.REPLICATION_INDEX_TRANSLOG_PRUNING_ENABLED_SETTING
        ) { value: Boolean -> translogPruningEnabled = value }

        indexSettings.scopedSettings.addSettingsUpdateConsumer(
            ReplicationPlugin.REPLICATION_INDEX_TRANSLOG_RETENTION_SIZE
        ) { value: ByteSizeValue -> if (translogPruningEnabled) retentionSizeInBytes = value.bytes }
    }

    override fun setRetentionSizeInBytes(bytes: Long) {
        if (!translogPruningEnabled) {
            retentionSizeInBytes = bytes
        }
    }

    override fun setRetentionAgeInMillis(ageInMillis: Long) {
        retentionAgeInMillis = ageInMillis
    }

    override fun setRetentionTotalFiles(retentionTotalFiles: Int) {
        this.retentionTotalFiles = retentionTotalFiles
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
        val minByAge = getMinTranslogGenByAge(readers, writer, retentionAgeInMillis, System.currentTimeMillis())
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