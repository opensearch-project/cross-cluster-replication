/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.replication.repository

import org.opensearch.index.store.StoreFileMetadata
import org.opensearch.indices.recovery.MultiChunkTransfer.ChunkRequest

class RemoteClusterRepositoryFileChunk constructor(
    val storeFileMetadata: StoreFileMetadata,
    val offset: Long,
    val length: Int,
) : ChunkRequest {

    override fun lastChunk(): Boolean {
        return storeFileMetadata.length() <= offset + length
    }
}
