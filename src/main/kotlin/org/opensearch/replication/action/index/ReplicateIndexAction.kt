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

package org.opensearch.replication.action.index

import org.opensearch.action.ActionType

class ReplicateIndexAction private constructor() : ActionType<ReplicateIndexResponse>(NAME, ::ReplicateIndexResponse) {
    companion object {
        const val NAME = "indices:admin/plugins/replication/index/start"
        val INSTANCE: ReplicateIndexAction = ReplicateIndexAction()
    }
}
