/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.replication.action.repository

import org.opensearch.action.ActionType

class GetStoreMetadataAction private constructor() : ActionType<GetStoreMetadataResponse>(NAME, ::GetStoreMetadataResponse) {
    companion object {
        const val NAME = "indices:data/read/plugins/replication/file_metadata"
        val INSTANCE = GetStoreMetadataAction()
    }
}
