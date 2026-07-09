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

import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput

class ReplicateIndexResponse(val ack: Boolean) : AcknowledgedResponse(ack) {
    constructor(inp: StreamInput) : this(inp.readBoolean())

    override fun writeTo(out: StreamOutput) {
        out.writeBoolean(ack)
    }
}
