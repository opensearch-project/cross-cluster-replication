package org.opensearch.index.translog

import org.opensearch.index.IndexSettings
import org.opensearch.index.IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING
import org.opensearch.index.seqno.RetentionLease
import org.opensearch.index.seqno.RetentionLeases
import org.opensearch.replication.ReplicationPlugin
import java.io.IOException
import java.util.function.Supplier

class ReplicationTranslogDeletionPolicy(
    private val indexSettings: IndexSettings,
    private val retentionLeasesSupplier: Supplier<RetentionLeases>
) : TranslogDeletionPolicy(
    indexSettings.translogRetentionSize.bytes,
    indexSettings.translogRetentionAge.millis,
    indexSettings.translogRetentionTotalFiles
) {

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
        var retentionSizeInBytes: Long = indexSettings.translogRetentionSize.bytes
        if (retentionSizeInBytes == -1L && indexSettings.settings.getAsBoolean(ReplicationPlugin.INDEX_TRANSLOG_RETENTION_LEASE_PRUNING_ENABLED_SETTING.key, false)) {
            retentionSizeInBytes = INDEX_TRANSLOG_RETENTION_SIZE_SETTING.get(indexSettings.settings).bytes
        }
        val minBySize: Long = getMinTranslogGenBySize(readers, writer, retentionSizeInBytes)
        val minByRetentionLeases: Long = getMinTranslogGenByRetentionLease(readers, writer)
        val minByTranslogGenSettings = super.minTranslogGenRequired(readers, writer)

        // If retention size is specified, size takes precedence.
        return Math.min(minByTranslogGenSettings, Math.max(minBySize, minByRetentionLeases))
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
            if(reader.getCheckpoint().minSeqNo <= minimumRetainingSequenceNumber &&
                reader.getCheckpoint().maxSeqNo >= minimumRetainingSequenceNumber) {
                minGen = Math.min(minGen, reader.getGeneration());
            }
        }
        return minGen;
    }
}