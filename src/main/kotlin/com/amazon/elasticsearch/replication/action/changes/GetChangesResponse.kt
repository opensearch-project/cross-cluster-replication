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

package com.amazon.elasticsearch.replication.action.changes

import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.index.translog.Translog

class GetChangesResponse(val changes: List<Translog.Operation>,
                         val fromSeqNo: Long,
                         val maxSeqNoOfUpdatesOrDeletes: Long,
                         val changesSizeEstimate: Long) : ActionResponse() {

    constructor(inp: StreamInput) : this(inp.readList(Translog.Operation::readOperation),
            inp.readVLong(), inp.readLong(), inp.readLong())

    override fun writeTo(out: StreamOutput) {
        out.writeCollection(changes, Translog.Operation::writeOperation)
        out.writeVLong(fromSeqNo)
        out.writeLong(maxSeqNoOfUpdatesOrDeletes)
        out.writeLong(changesSizeEstimate)
    }
}
