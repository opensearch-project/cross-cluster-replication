/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.replication.action.changes

import org.opensearch.action.ActionType

class GetChangesAction private constructor() : ActionType<GetChangesResponse>(NAME, ::GetChangesResponse) {

    companion object {
        const val NAME = "indices:data/read/plugins/replication/changes"
        val INSTANCE = GetChangesAction()
    }
}
