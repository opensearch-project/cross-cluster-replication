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

package org.opensearch.replication.action.repository

import org.opensearch.action.ActionResponse
import org.opensearch.common.bytes.BytesReference
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.index.store.StoreFileMetadata

class GetFileChunkResponse : ActionResponse {

    val storeFileMetadata: StoreFileMetadata
    val offset: Long
    val data: BytesReference

    constructor(storeFileMetadata: StoreFileMetadata, offset: Long, data: BytesReference): super() {
        this.storeFileMetadata = storeFileMetadata
        this.offset = offset
        this.data = data
    }

    constructor(inp: StreamInput): super(inp) {
        storeFileMetadata = StoreFileMetadata(inp)
        offset = inp.readLong()
        data = inp.readBytesReference()
    }

    override fun writeTo(out: StreamOutput) {
        storeFileMetadata.writeTo(out)
        out.writeLong(offset)
        out.writeBytesReference(data)
    }
}
