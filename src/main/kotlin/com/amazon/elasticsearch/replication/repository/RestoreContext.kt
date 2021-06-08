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

package com.amazon.elasticsearch.replication.repository

import com.amazon.elasticsearch.replication.util.performOp
import org.apache.lucene.store.IOContext
import org.apache.lucene.store.IndexInput
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.index.engine.Engine
import org.elasticsearch.index.shard.IndexShard
import org.elasticsearch.index.store.Store
import java.io.Closeable

class RestoreContext(val restoreUUID: String,
                          val shard: IndexShard,
                          val indexCommitRef: Engine.IndexCommitRef,
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
            currentIndexInput = store.directory().openInput(fileName, IOContext.READONCE)
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
