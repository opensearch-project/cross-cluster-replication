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

import org.opensearch.action.ActionType

class GetFileChunkAction private constructor() : ActionType<GetFileChunkResponse>(NAME, ::GetFileChunkResponse) {
    companion object {
        const val NAME = "indices:data/read/plugins/replication/file_chunk"
        val INSTANCE = GetFileChunkAction()
    }
}
