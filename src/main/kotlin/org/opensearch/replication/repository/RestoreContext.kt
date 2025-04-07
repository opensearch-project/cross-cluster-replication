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

package org.opensearch.replication.repository

import org.apache.lucene.index.IndexCommit
import org.apache.lucene.store.IOContext
import org.apache.lucene.store.IndexInput
import org.opensearch.common.concurrent.GatedCloseable
import org.opensearch.index.shard.IndexShard
import org.opensearch.index.store.Store
import java.io.Closeable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class RestoreContext(
    val restoreUUID: String,
    val shard: IndexShard,
    val indexCommitRef: GatedCloseable<IndexCommit>,
    val metadataSnapshot: Store.MetadataSnapshot,
    val replayOperationsFrom: Long,
) : Closeable {

    private val currentFiles = ConcurrentHashMap<String, IndexInput>(INITIAL_FILE_CACHE_CAPACITY)

    private val fileLocks = ConcurrentHashMap<String, ReentrantLock>()

    fun openInput(store: Store, fileName: String): IndexInput {
        val lock = fileLocks.computeIfAbsent(fileName) { ReentrantLock() }

        lock.withLock {
            val baseInput = currentFiles.computeIfAbsent(fileName) {
                store.directory().openInput(fileName, IOContext.DEFAULT)
            }
            return baseInput.clone()
        }
    }

    override fun close() {
        currentFiles.values.forEach { it.close() }
        currentFiles.clear()
        fileLocks.clear()
        indexCommitRef.close()
    }

    companion object {
        private const val INITIAL_FILE_CACHE_CAPACITY = 20
    }
}
