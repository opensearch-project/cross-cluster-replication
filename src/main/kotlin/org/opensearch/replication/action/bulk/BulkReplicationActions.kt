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

package org.opensearch.replication.action.bulk

import org.opensearch.action.ActionType

class BulkStartReplicationAction private constructor() :
    ActionType<BulkReplicationResponse>(NAME, ::BulkReplicationResponse) {
    companion object {
        const val NAME = "cluster:admin/plugins/replication/bulk/start"
        val INSTANCE = BulkStartReplicationAction()
    }
}

class BulkStopReplicationAction private constructor() :
    ActionType<BulkReplicationResponse>(NAME, ::BulkReplicationResponse) {
    companion object {
        const val NAME = "cluster:admin/plugins/replication/bulk/stop"
        val INSTANCE = BulkStopReplicationAction()
    }
}

class BulkPauseReplicationAction private constructor() :
    ActionType<BulkReplicationResponse>(NAME, ::BulkReplicationResponse) {
    companion object {
        const val NAME = "cluster:admin/plugins/replication/bulk/pause"
        val INSTANCE = BulkPauseReplicationAction()
    }
}

class BulkResumeReplicationAction private constructor() :
    ActionType<BulkReplicationResponse>(NAME, ::BulkReplicationResponse) {
    companion object {
        const val NAME = "cluster:admin/plugins/replication/bulk/resume"
        val INSTANCE = BulkResumeReplicationAction()
    }
}
