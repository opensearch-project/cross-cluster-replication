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

import org.opensearch.index.store.StoreFileMetadata
import org.opensearch.indices.recovery.MultiChunkTransfer.ChunkRequest

class RemoteClusterRepositoryFileChunk constructor(val storeFileMetadata: StoreFileMetadata,
                                                   val offset: Long,
                                                   val length: Int): ChunkRequest {

    override fun lastChunk(): Boolean {
        return storeFileMetadata.length() <= offset + length
    }
}
