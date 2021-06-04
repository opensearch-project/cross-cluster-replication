/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.replication.action.replicationstatedetails

import org.opensearch.replication.metadata.ReplicationStateParams
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.support.master.AcknowledgedRequest
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput

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
        out.writeMap(replicationStateParams)
    }
}