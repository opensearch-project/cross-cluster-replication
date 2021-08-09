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

package org.opensearch.replication.action.status

import org.opensearch.action.ActionType
import org.opensearch.common.io.stream.Writeable

class ReplicationStatusAction : ActionType<ReplicationStatusResponse>(NAME, reader) {
    companion object {
        const val NAME = "indices:admin/plugins/replication/index/status_check"
        val INSTANCE = ReplicationStatusAction()
        val reader = Writeable.Reader { inp -> ReplicationStatusResponse(inp) }
    }

    override fun getResponseReader(): Writeable.Reader<ReplicationStatusResponse> = reader
}
