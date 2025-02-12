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

package org.opensearch.replication.action.replicationstatedetails

import org.opensearch.replication.metadata.state.ReplicationStateParams
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.support.clustermanager.AcknowledgedRequest
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput

class UpdateReplicationStateDetailsRequest: AcknowledgedRequest<UpdateReplicationStateDetailsRequest> {

    val followIndexName : String
    val replicationStateParams : ReplicationStateParams
    val updateType : UpdateType

    enum class UpdateType {
        ADD, REMOVE
    }

    constructor(followIndexName : String,
                replicationStateParams: ReplicationStateParams,
                updateType: UpdateType) {
        this.followIndexName = followIndexName
        this.replicationStateParams = replicationStateParams
        this.updateType = updateType
    }

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    constructor(inp: StreamInput) : super(inp) {
        followIndexName = inp.readString()
        replicationStateParams = inp.readMap(StreamInput::readString, StreamInput::readString)
        updateType = inp.readEnum(UpdateType::class.java)
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(followIndexName)
        out.writeMap(replicationStateParams, StreamOutput::writeString, StreamOutput::writeString)
        out.writeEnum(updateType)
    }
}