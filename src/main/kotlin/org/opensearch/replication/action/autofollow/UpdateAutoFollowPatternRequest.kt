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

package org.opensearch.replication.action.autofollow

import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.support.master.AcknowledgedRequest
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken

class UpdateAutoFollowPatternRequest: AcknowledgedRequest<UpdateAutoFollowPatternRequest> {

    companion object {
        fun fromXContent(xcp: XContentParser, action: Action) : UpdateAutoFollowPatternRequest {
            var connection: String? = null
            var patternName: String? = null
            var pattern: String? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                when (fieldName) {
                    "connection" -> connection = xcp.text()
                    "name"       -> patternName = xcp.text()
                    "pattern"    -> pattern = xcp.textOrNull()
                }
            }
            requireNotNull(connection) { "missing connection" }
            requireNotNull(patternName) { "missing pattern name" }
            if (action == Action.REMOVE) {
                require(pattern == null) { "unexpected pattern provided" }
            } else {
                requireNotNull(pattern) { "missing pattern" }
            }
            return UpdateAutoFollowPatternRequest(connection, patternName, pattern, action)
        }
    }
    val connection: String
    val patternName: String
    val pattern: String?

    enum class Action {
        ADD, REMOVE
    }
    val action : Action

    constructor(connection: String, patternName: String, pattern: String?, action: Action) {
        this.connection = connection
        this.patternName = patternName
        this.pattern = pattern
        this.action = action
    }

    constructor(inp: StreamInput) : super(inp) {
        connection = inp.readString()
        patternName = inp.readString()
        pattern = inp.readOptionalString()
        action = inp.readEnum(Action::class.java)
    }


    override fun validate(): ActionRequestValidationException? = null

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(connection)
        out.writeString(patternName)
        out.writeOptionalString(pattern)
        out.writeEnum(action)
    }
}