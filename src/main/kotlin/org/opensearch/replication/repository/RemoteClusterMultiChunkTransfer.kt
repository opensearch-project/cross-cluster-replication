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

import org.opensearch.replication.action.repository.GetFileChunkAction
import org.opensearch.replication.action.repository.GetFileChunkRequest
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.logging.log4j.Logger
import org.opensearch.action.ActionListener
import org.opensearch.client.Client
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.common.unit.ByteSizeValue
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.index.shard.ShardId
import org.opensearch.index.store.Store
import org.opensearch.index.store.StoreFileMetadata
import org.opensearch.indices.recovery.MultiChunkTransfer
import org.opensearch.indices.recovery.MultiFileWriter
import org.opensearch.indices.recovery.RecoveryState
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.suspendExecute

class RemoteClusterMultiChunkTransfer(val logger: Logger,
                                      val followerClusterName: String,
                                      threadContext: ThreadContext,
                                      val localStore: Store,
                                      maxConcurrentFileChunks: Int,
                                      val restoreUUID: String,
                                      val remoteNode: DiscoveryNode,
                                      val remoteShardId: ShardId,
                                      val remoteFiles: List<StoreFileMetadata>,
                                      val remoteClusterClient: Client,
                                      val recoveryState: RecoveryState,
                                      val chunkSize: ByteSizeValue,
                                      listener: ActionListener<Void>) :
        MultiChunkTransfer<StoreFileMetadata, RemoteClusterRepositoryFileChunk>(logger,
                threadContext, listener, maxConcurrentFileChunks, remoteFiles), CoroutineScope by GlobalScope {

    private var offset = 0L
    private val tempFilePrefix = "${RESTORE_SHARD_TEMP_FILE_PREFIX}${restoreUUID}."
    private val multiFileWriter = MultiFileWriter(localStore, recoveryState.index, tempFilePrefix, logger) {}
    private val mutex = Mutex()

    init {
        // Add all the available files to show the recovery status
        for(fileMetadata in remoteFiles) {
            recoveryState.index.addFileDetail(fileMetadata.name(), fileMetadata.length(), false)
        }
        recoveryState.index.setFileDetailsComplete()
    }

    companion object {
        const val RESTORE_SHARD_TEMP_FILE_PREFIX = "CLUSTER_REPO_TEMP_"
    }

    override fun handleError(md: StoreFileMetadata, e: Exception) {
        logger.error("Error while transferring segments $e")
    }

    override fun onNewResource(md: StoreFileMetadata) {
        // Reset the values for the next file
        offset = 0L
    }

    override fun executeChunkRequest(request: RemoteClusterRepositoryFileChunk, listener: ActionListener<Void>) {
        val getFileChunkRequest = GetFileChunkRequest(restoreUUID, remoteNode, remoteShardId, request.storeFileMetadata,
                request.offset, request.length, followerClusterName, recoveryState.shardId)

        launch(Dispatchers.IO + remoteClusterClient.threadPool().coroutineContext()) {
            try {
                val response = remoteClusterClient.suspendExecute(GetFileChunkAction.INSTANCE, getFileChunkRequest)
                logger.debug("Filename: ${request.storeFileMetadata.name()}, " +
                        "response_size: ${response.data.length()}, response_offset: ${response.offset}")
                mutex.withLock {
                    multiFileWriter.writeFileChunk(response.storeFileMetadata, response.offset, response.data, request.lastChunk())
                    listener.onResponse(null)
                }
            } catch (e: Exception) {
                logger.error("Failed to fetch file chunk for ${request.storeFileMetadata.name()} with offset ${request.offset}: $e")
                listener.onFailure(e)
            }
        }

    }

    override fun nextChunkRequest(md: StoreFileMetadata): RemoteClusterRepositoryFileChunk {
        val chunkReq = RemoteClusterRepositoryFileChunk(md, offset, chunkSize.bytesAsInt())
        offset += chunkSize.bytesAsInt()
        return chunkReq
    }

    override fun close() {
        multiFileWriter.renameAllTempFiles()
        multiFileWriter.close()
    }
}
