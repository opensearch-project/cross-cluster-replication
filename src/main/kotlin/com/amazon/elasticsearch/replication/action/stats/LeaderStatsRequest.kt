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

package com.amazon.elasticsearch.replication.action.stats

import org.elasticsearch.action.support.nodes.BaseNodesRequest
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import java.io.IOException

/**
 * A request to get node (cluster) level replication stats.
 */
class LeaderStatsRequest : BaseNodesRequest<LeaderStatsRequest?> {

    /**
     *
     * @param in A stream input object.
     * @throws IOException if the stream cannot be deserialized.
     */
    constructor(inp: StreamInput) : super(inp)

    /**
     * Get information from nodes based on the nodes ids specified. If none are passed, information
     * for all nodes will be returned.
     */
    constructor(vararg nodesIds: String) : super(*nodesIds)

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
    }

}
