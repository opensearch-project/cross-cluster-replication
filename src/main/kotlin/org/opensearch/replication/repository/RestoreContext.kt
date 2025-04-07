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
import org.opensearch.replication.util.performOp
import org.apache.lucene.store.IOContext
import org.apache.lucene.store.IndexInput
import org.opensearch.OpenSearchException
import org.opensearch.common.concurrent.GatedCloseable
import org.opensearch.index.engine.Engine
import org.opensearch.index.shard.IndexShard
import org.opensearch.index.store.Store
import java.io.Closeable

class RestoreContext(val restoreUUID: String,
                     val shard: IndexShard,
                     val indexCommitRef: GatedCloseable<IndexCommit>,
                     val metadataSnapshot: Store.MetadataSnapshot,
                     val replayOperationsFrom: Long): Closeable {

    companion object {
        private const val INITIAL_FILE_CACHE_CAPACITY = 20
    }
    private val currentFiles = LinkedHashMap<String, IndexInput>(INITIAL_FILE_CACHE_CAPACITY)

    fun openInput(store: Store, fileName: String): IndexInput {
        var currentIndexInput = currentFiles.getOrDefault(fileName, null)
        if(currentIndexInput != null) {
            return currentIndexInput.clone()
        }
        store.performOp({
            currentIndexInput = store.directory().openInput(fileName, IOContext.DEFAULT)
        })

        currentFiles[fileName] = currentIndexInput!!
        return currentIndexInput!!.clone()
    }

    override fun close() {
        // Close all the open index input obj
        currentFiles.entries.forEach {
            it.value.close()
        }
        currentFiles.clear()
        indexCommitRef.close()
    }

}
