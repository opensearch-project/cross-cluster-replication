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

package org.opensearch.replication.repository

import org.opensearch.replication.action.repository.RemoteClusterRepositoryRequest
import org.opensearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import org.opensearch.OpenSearchException
import org.opensearch.action.support.single.shard.SingleShardRequest
import org.opensearch.client.node.NodeClient
import org.opensearch.common.component.AbstractLifecycleComponent
import org.opensearch.common.inject.Inject
import org.opensearch.common.inject.Singleton
import org.opensearch.common.lucene.store.InputStreamIndexInput
import org.opensearch.core.internal.io.IOUtils
import org.opensearch.index.engine.Engine
import org.opensearch.index.seqno.RetentionLeaseActions
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.index.shard.IndexShard
import org.opensearch.index.shard.ShardId
import org.opensearch.index.store.Store
import org.opensearch.indices.IndicesService
import org.opensearch.replication.util.performOp
import java.io.Closeable
import java.io.IOException

/*
 * Restore source service tracks all the ongoing restore operations
 * relying on the leader shards. Once the restore is completed the
 * relevant resources are released. Also, listens on the index events
 * to update the resources
 */
@Singleton
class RemoteClusterRestoreLeaderService @Inject constructor(private val indicesService: IndicesService,
                                                    private val nodeClient : NodeClient) :
        AbstractLifecycleComponent() {

    // TODO: Listen for the index events and release relevant resources.
    private val onGoingRestores: MutableMap<String, RestoreContext> = mutableMapOf()
    private val closableResources: MutableList<Closeable> = mutableListOf()

    override fun doStart() {
    }

    override fun doStop() {
    }

    override fun doClose() {
        // Obj in the list being null or closed has no effect
        IOUtils.close(closableResources)
    }

    @Synchronized
    fun <T : SingleShardRequest<T>?> addRemoteClusterRestore(restoreUUID: String,
                                                             request: RemoteClusterRepositoryRequest<T>): RestoreContext {
        return onGoingRestores.getOrPut(restoreUUID) { constructRestoreContext(restoreUUID, request)}
    }

    private fun getRemoteClusterRestore(restoreUUID: String): RestoreContext {
        return onGoingRestores[restoreUUID] ?: throw IllegalStateException("missing restoreContext")
    }

    @Synchronized
    fun <T : SingleShardRequest<T>?> openInputStream(restoreUUID: String,
                                                     request: RemoteClusterRepositoryRequest<T>,
                                                     fileName: String,
                                                     length: Long): InputStreamIndexInput {
        val leaderIndexShard = indicesService.getShardOrNull(request.leaderShardId)
                ?: throw OpenSearchException("Shard [$request.leaderShardId] missing")
        val store = leaderIndexShard.store()
        val restoreContext = getRemoteClusterRestore(restoreUUID)
        val indexInput = restoreContext.openInput(store, fileName)

        return object : InputStreamIndexInput(indexInput, length) {
            @Throws(IOException::class)
            override fun close() {
                IOUtils.close(indexInput, Closeable { super.close() }) // InputStreamIndexInput's close is a noop
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
        val retentionLock = leaderIndexShard.acquireHistoryRetentionLock(Engine.HistorySource.INDEX)
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
            metadataSnapshot = store.getMetadata(indexCommitRef.indexCommit)
        })

        // Identifies the seq no to start the replication operations from
        var fromSeqNo = RetentionLeaseActions.RETAIN_ALL

        // Adds the retention lease for fromSeqNo for the next stage of the replication.
        retentionLeaseHelper.addRetentionLease(request.leaderShardId, fromSeqNo,
                request.followerShardId, RemoteClusterRepository.REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC)

        /**
         * At this point, it should be safe to release retention lock as the retention lease
         * is acquired from the local checkpoint and the rest of the follower replay actions
         * can be performed using this retention lease.
         */
        retentionLock.close()

        var restoreContext = RestoreContext(restoreUUID, leaderIndexShard,
                indexCommitRef, metadataSnapshot, fromSeqNo)
        onGoingRestores[restoreUUID] = restoreContext

        closableResources.add(restoreContext)
        return restoreContext
    }

    @Synchronized
    fun removeRemoteClusterRestore(restoreUUID: String) {
        val restoreContext = onGoingRestores.remove(restoreUUID)
        /**
         * cleaning the resources - Closing only index safe commit
         * as retention lease will be updated in the GetChanges flow
         */
        restoreContext?.close()
    }
}
