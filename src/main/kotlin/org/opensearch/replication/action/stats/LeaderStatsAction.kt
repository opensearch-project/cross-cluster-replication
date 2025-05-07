/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.replication.action.stats

import org.opensearch.action.ActionType
import org.opensearch.core.common.io.stream.Writeable

class LeaderStatsAction : ActionType<LeaderStatsResponse>(NAME, reader) {
    companion object {
        const val NAME = "indices:admin/plugins/replication/index/stats"
        val INSTANCE = LeaderStatsAction()
        val reader = Writeable.Reader { inp -> LeaderStatsResponse(inp) }
    }

    override fun getResponseReader(): Writeable.Reader<LeaderStatsResponse> = reader
}
