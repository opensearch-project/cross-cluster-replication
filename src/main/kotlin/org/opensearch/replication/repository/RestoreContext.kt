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
import org.opensearch.common.util.concurrent.AbstractRefCounted
import org.opensearch.commons.utils.OpenForTesting
import org.opensearch.core.index.shard.ShardId
import org.opensearch.index.shard.IndexShard
import org.opensearch.index.store.Store
import org.opensearch.replication.util.performOp
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Tracks the resources held on the leader for one bootstrap restore session (safe-commit ref and the
 * per-file [IndexInput]s served to the follower). Reference counted: the base ref is held by the leader
 * service's session map, and every in-flight [openInput] read holds an additional ref, so the underlying
 * store inputs and commit ref are released only after both the session ends AND all reads drain. This lets
 * idle-session eviction reclaim a slot immediately without closing an input mid-read.
 *
 * [followerCluster]/[followerShardId] identify the retention lease to release when a session is evicted
 * (a dead follower will never reach the GetChanges flow that would otherwise take the lease over).
 */
@OpenForTesting
open class RestoreContext(
    val restoreUUID: String,
    val shard: IndexShard,
    val indexCommitRef: GatedCloseable<IndexCommit>,
    val metadataSnapshot: Store.MetadataSnapshot,
    val replayOperationsFrom: Long,
    val followerCluster: String = "",
    val followerShardId: ShardId? = null,
) : AbstractRefCounted("restore-context-$restoreUUID") {

    @Volatile
    var lastAccessMillis: Long = 0L
        private set

    private val currentFiles = ConcurrentHashMap<String, IndexInput>(INITIAL_FILE_CACHE_CAPACITY)

    private val fileLocks = ConcurrentHashMap<String, ReentrantLock>()

    /** Records that the session was touched at [now] (relative clock millis) so it is not evicted as idle. */
    fun touch(now: Long) {
        lastAccessMillis = now
    }

    fun openInput(store: Store, fileName: String): IndexInput {
        val lock = fileLocks.computeIfAbsent(fileName) { ReentrantLock() }

        lock.withLock {
            var baseInput: IndexInput? = null

            withStoreReference(store) {
                baseInput = currentFiles.computeIfAbsent(fileName) {
                    store.directory().openInput(fileName, IOContext.DEFAULT)
                }
            }

            return checkNotNull(baseInput) { "[RestoreContext] IndexInput file must not be null" }.clone()
        }
    }

    // for testing
    internal open fun withStoreReference(store: Store, block: () -> Unit) {
        store.performOp(block)
    }

    override fun closeInternal() {
        currentFiles.values.forEach { it.close() }
        currentFiles.clear()
        fileLocks.clear()
        indexCommitRef.close()
    }

    companion object {
        private const val INITIAL_FILE_CACHE_CAPACITY = 20
    }
}

