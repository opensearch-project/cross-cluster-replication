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

import org.opensearch.replication.ReplicationPlugin
import org.opensearch.replication.action.repository.RemoteClusterRepositoryRequest
import org.opensearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import org.opensearch.replication.util.performOp
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchException
import org.opensearch.action.support.single.shard.SingleShardRequest
import org.opensearch.transport.client.node.NodeClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.lifecycle.AbstractLifecycleComponent
import org.opensearch.common.inject.Inject
import org.opensearch.common.inject.Singleton
import org.opensearch.common.lucene.store.InputStreamIndexInput
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.util.io.IOUtils
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException
import org.opensearch.index.seqno.RetentionLeaseActions
import org.opensearch.index.store.Store
import org.opensearch.indices.IndicesService
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool
import java.io.Closeable
import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean

/*
 * Restore source service tracks all the ongoing restore operations
 * relying on the leader shards. Once the restore is completed the
 * relevant resources are released. Also, listens on the index events
 * to update the resources
 */
@Singleton
class RemoteClusterRestoreLeaderService @Inject constructor(private val indicesService: IndicesService,
                                                            private val nodeClient : NodeClient,
                                                            private val threadPool: ThreadPool,
                                                            private val clusterService: ClusterService) :
        AbstractLifecycleComponent() {

    // TODO: Listen for the index events and release relevant resources.
    private val onGoingRestores: MutableMap<String, RestoreContext> = mutableMapOf()
    private val closableResources: MutableList<Closeable> = mutableListOf()

    @Volatile private var maxConcurrentRecoveries =
            clusterService.clusterSettings.get(ReplicationPlugin.REPLICATION_LEADER_RESTORE_MAX_CONCURRENT_RECOVERIES)
    @Volatile private var sessionIdleTimeout =
            clusterService.clusterSettings.get(ReplicationPlugin.REPLICATION_LEADER_RESTORE_SESSION_IDLE_TIMEOUT)
    private var evictionTask: Scheduler.Cancellable? = null

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_LEADER_RESTORE_MAX_CONCURRENT_RECOVERIES) { maxConcurrentRecoveries = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_LEADER_RESTORE_SESSION_IDLE_TIMEOUT) { sessionIdleTimeout = it }
    }

    companion object {
        private val log = LogManager.getLogger(RemoteClusterRestoreLeaderService::class.java)
        private val EVICTION_INTERVAL = TimeValue.timeValueSeconds(30)
    }

    override fun doStart() {
        evictionTask = threadPool.scheduleWithFixedDelay({ evictIdleRestores() }, EVICTION_INTERVAL, ThreadPool.Names.GENERIC)
    }

    override fun doStop() {
        evictionTask?.cancel()
    }

    override fun doClose() {
        // Obj in the list being null or closed has no effect
        IOUtils.close(closableResources)
        synchronized(this) {
            onGoingRestores.values.forEach { it.decRef() }
            onGoingRestores.clear()
        }
    }

    @Synchronized
    fun <T : SingleShardRequest<T>?> addLeaderClusterRestore(restoreUUID: String,
                                                             request: RemoteClusterRepositoryRequest<T>): RestoreContext {
        val existing = onGoingRestores[restoreUUID]
        if (existing != null) {
            existing.touch(threadPool.relativeTimeInMillis())
            return existing
        }
        // Admission control: reject new sessions past the per-node cap with a retryable 429. This bounds the
        // leader heap held by in-flight chunk responses and is what the follower retries against (back-off loop).
        if (onGoingRestores.size >= maxConcurrentRecoveries) {
            throw OpenSearchRejectedExecutionException(
                    "Leader restore sessions at capacity [$maxConcurrentRecoveries]; retry after in-flight restores drain")
        }
        val restoreContext = constructRestoreContext(restoreUUID, request)
        restoreContext.touch(threadPool.relativeTimeInMillis())
        return restoreContext
    }

    private fun getLeaderClusterRestore(restoreUUID: String): RestoreContext {
        return onGoingRestores[restoreUUID] ?: throw IllegalStateException("missing restoreContext")
    }

    @Synchronized
    fun <T : SingleShardRequest<T>?> openInputStream(restoreUUID: String,
                                                     request: RemoteClusterRepositoryRequest<T>,
                                                     fileName: String,
                                                     offset: Long,
                                                     length: Long): InputStreamIndexInput {
        val leaderIndexShard = indicesService.getShardOrNull(request.leaderShardId)
                ?: throw OpenSearchException("Shard [$request.leaderShardId] missing")
        val store = leaderIndexShard.store()
        val restoreContext = getLeaderClusterRestore(restoreUUID)
        // Hold a read ref for the life of the returned stream so idle-eviction or completion cannot close the
        // underlying store inputs mid-read; the ref is released exactly once when the stream is closed.
        restoreContext.incRef()
        restoreContext.touch(threadPool.relativeTimeInMillis())
        var streamCreated = false
        try {
            val indexInput = restoreContext.openInput(store, fileName)

            /**
             * Seek directly to the requested chunk offset on the (cloned) IndexInput instead of
             * relying on InputStream.skip, which is a read-and-discard loop. Skipping made serving
             * chunk k cost O(k * chunkSize) of leader-side reads, i.e. O(N^2) per file transfer.
             * The clone is per-request, so seeking it is safe under concurrent chunk fetches.
             */
            if (offset > 0) {
                indexInput.seek(offset)
            }

            // Bound the stream to the bytes remaining after the seek.
            val stream = object : InputStreamIndexInput(indexInput, length - offset) {
                private val closed = AtomicBoolean(false)
                @Throws(IOException::class)
                override fun close() {
                    if (closed.compareAndSet(false, true)) {
                        try {
                            IOUtils.close(indexInput, Closeable { super.close() }) // InputStreamIndexInput's close is a noop
                        } finally {
                            restoreContext.decRef()
                        }
                    }
                }
            }
            streamCreated = true
            return stream
        } finally {
            // If we failed before handing the stream (and its close) to the caller, release the read ref now.
            if (!streamCreated) {
                restoreContext.decRef()
            }
        }
    }

    private fun <T : SingleShardRequest<T>?> constructRestoreContext(restoreUUID: String,
                                        request: RemoteClusterRepositoryRequest<T>): RestoreContext {
        val leaderIndexShard = indicesService.getShardOrNull(request.leaderShardId)
                ?: throw OpenSearchException("Shard [$request.leaderShardId] missing")
        // Passing nodeclient of the leader to acquire the retention lease on leader shard
        val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(request.followerCluster, nodeClient)
        /**
         * ODFE Replication supported for >= ES 7.8. History of operations directly from
         * lucene index. With the retention lock set - safe commit should have all the history
         * upto the current retention leases.
         */
        val retentionLock = leaderIndexShard.acquireHistoryRetentionLock()
        closableResources.add(retentionLock)

        /**
         * Construct restore via safe index commit
         * at the leader cluster. All the references from this commit
         * should be available until it is closed.
         */
        val indexCommitRef = leaderIndexShard.acquireSafeIndexCommit()

        val store = leaderIndexShard.store()
        var metadataSnapshot = Store.MetadataSnapshot.EMPTY
        store.performOp({
            metadataSnapshot = store.getMetadata(indexCommitRef.get())
        })

        // Identifies the seq no to start the replication operations from
        var fromSeqNo = RetentionLeaseActions.RETAIN_ALL

        // Adds the retention lease for fromSeqNo for the next stage of the replication.
        retentionLeaseHelper.addRetentionLease(request.leaderShardId, fromSeqNo, request.followerShardId,
                RemoteClusterRepository.REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC)

        /**
         * At this point, it should be safe to release retention lock as the retention lease
         * is acquired from the local checkpoint and the rest of the follower replay actions
         * can be performed using this retention lease.
         */
        retentionLock.close()

        var restoreContext = RestoreContext(restoreUUID, leaderIndexShard,
                indexCommitRef, metadataSnapshot, fromSeqNo, request.followerCluster, request.followerShardId)
        onGoingRestores[restoreUUID] = restoreContext

        return restoreContext
    }

    @Synchronized
    fun removeLeaderClusterRestore(restoreUUID: String) {
        val restoreContext = onGoingRestores.remove(restoreUUID)
        /**
         * cleaning the resources - dropping the base ref releases the safe index commit and cached inputs once
         * in-flight reads drain. The retention lease is intentionally left in place on normal completion, as it
         * will be taken over by the GetChanges flow.
         */
        restoreContext?.decRef()
    }

    /**
     * Periodically drops restore sessions that have not been touched within [sessionIdleTimeout], reclaiming
     * their slot against [maxConcurrentRecoveries]. Unlike normal completion, eviction implies the follower
     * abandoned the restore (it will never reach the GetChanges flow), so the retention lease is actively
     * removed to avoid pinning history on the leader. Resource release is done outside the monitor.
     */
    private fun evictIdleRestores() {
        val now = threadPool.relativeTimeInMillis()
        val timeoutMillis = sessionIdleTimeout.millis()
        val evicted = mutableMapOf<String, RestoreContext>()
        synchronized(this) {
            val iterator = onGoingRestores.entries.iterator()
            while (iterator.hasNext()) {
                val entry = iterator.next()
                if (now - entry.value.lastAccessMillis <= timeoutMillis) continue
                iterator.remove()
                evicted[entry.key] = entry.value
            }
        }
        for ((uuid, context) in evicted) {
            log.warn("Evicting idle leader restore session [$uuid], idle for more than ${timeoutMillis}ms")
            val followerShardId = context.followerShardId
            if (followerShardId != null && context.followerCluster.isNotEmpty()) {
                try {
                    RemoteClusterRetentionLeaseHelper(context.followerCluster, nodeClient)
                            .attemptRetentionLeaseRemoval(context.shard.shardId(), followerShardId,
                                    RemoteClusterRepository.REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC)
                } catch (e: Exception) {
                    log.error("Failed to remove retention lease while evicting restore session [$uuid]", e)
                }
            }
            context.decRef()
        }
    }
}
