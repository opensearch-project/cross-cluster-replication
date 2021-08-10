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

package org.opensearch.replication.action.replay

import org.opensearch.action.support.WriteResponse
import org.opensearch.action.support.replication.ReplicationResponse
import org.opensearch.common.io.stream.StreamInput

class ReplayChangesResponse : ReplicationResponse, WriteResponse {

    constructor(inp: StreamInput) : super(inp)

    constructor(): super()

    override fun setForcedRefresh(forcedRefresh: Boolean) {
        //no-op
    }


}