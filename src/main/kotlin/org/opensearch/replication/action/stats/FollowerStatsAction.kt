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

package org.opensearch.replication.action.stats

import org.opensearch.action.ActionType
import org.opensearch.core.common.io.stream.Writeable

class FollowerStatsAction : ActionType<FollowerStatsResponse>(NAME, reader) {
    companion object {
        const val NAME = "indices:admin/plugins/replication/follower/stats"
        val INSTANCE = FollowerStatsAction()
        val reader = Writeable.Reader { inp -> FollowerStatsResponse(inp) }
    }

    override fun getResponseReader(): Writeable.Reader<FollowerStatsResponse> = reader
}
