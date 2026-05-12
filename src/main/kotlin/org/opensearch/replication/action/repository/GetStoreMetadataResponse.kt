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

package org.opensearch.replication.action.repository

import org.opensearch.core.action.ActionResponse
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.index.store.Store

class GetStoreMetadataResponse : ActionResponse {

    val metadataSnapshot : Store.MetadataSnapshot

    constructor(metadataSnapshot: Store.MetadataSnapshot): super() {
        this.metadataSnapshot = metadataSnapshot
    }

    constructor(inp: StreamInput) : super(inp) {
        metadataSnapshot = Store.MetadataSnapshot(inp)
    }

    override fun writeTo(out: StreamOutput) {
        metadataSnapshot.writeTo(out)
    }
}
