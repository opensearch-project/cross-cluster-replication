/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.replication.action.replay

import org.opensearch.action.support.WriteResponse
import org.opensearch.action.support.replication.ReplicationResponse
import org.opensearch.core.common.io.stream.StreamInput

class ReplayChangesResponse : ReplicationResponse, WriteResponse {

    constructor(inp: StreamInput) : super(inp)

    constructor() : super()

    override fun setForcedRefresh(forcedRefresh: Boolean) {
        // no-op
    }
}
