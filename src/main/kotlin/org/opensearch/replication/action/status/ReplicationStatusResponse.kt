/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.replication.action.status

import org.opensearch.action.support.broadcast.BroadcastResponse
import org.opensearch.core.action.support.DefaultShardOperationFailedException
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent.Params
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import java.io.IOException

class ReplicationStatusResponse : BroadcastResponse, ToXContentObject {

    lateinit var shardInfoResponse: MutableList<ShardInfoResponse>
    lateinit var status: String
    lateinit var reason: String
    lateinit var connectionAlias: String
    lateinit var leaderIndexName: String
    lateinit var followerIndexName: String
    lateinit var aggregatedReplayDetails: ReplayDetails
    lateinit var aggregatedRestoreDetails: RestoreDetails
    var isVerbose: Boolean = true

    @Throws(IOException::class)
    constructor(inp: StreamInput) : super(inp) {
        shardInfoResponse = inp.readList(::ShardInfoResponse)
    }

    constructor(
        totalShards: Int,
        successfulShards: Int,
        failedShards: Int,
        shardFailures: List<DefaultShardOperationFailedException>,
        shardInfoRespons: List<ShardInfoResponse>,
    ) : super(
        totalShards, successfulShards, failedShards, shardFailures,
    ) {
        this.shardInfoResponse = shardInfoRespons.toMutableList()
    }

    constructor(
        status: String,
    ) {
        this.status = status
    }

    constructor(
        totalShards: Int,
        successfulShards: Int,
        failedShards: Int,
        shardFailures: List<DefaultShardOperationFailedException>,
        shardInfoResponse: List<ShardInfoResponse>,
        status: String,
        reason: String,
    ) : super(
        totalShards, successfulShards, failedShards, shardFailures,
    ) {
        this.shardInfoResponse = shardInfoResponse.toMutableList()
        this.status = status
        this.reason = reason
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: Params?): XContentBuilder {
        builder.startObject()
        if (::status.isInitialized) {
            builder.field("status", status)
        }
        if (::reason.isInitialized) {
            builder.field("reason", reason)
        }
        if (::connectionAlias.isInitialized) {
            builder.field("leader_alias", connectionAlias)
        }
        if (::leaderIndexName.isInitialized) {
            builder.field("leader_index", leaderIndexName)
        }
        if (::followerIndexName.isInitialized) {
            builder.field("follower_index", followerIndexName)
        }
        if (::aggregatedReplayDetails.isInitialized) {
            builder.field("syncing_details", aggregatedReplayDetails)
        }
        if (::aggregatedRestoreDetails.isInitialized) {
            builder.field("bootstrap_details", aggregatedRestoreDetails)
        }
        if (isVerbose and ::shardInfoResponse.isInitialized) {
            builder.field("shard_replication_details", shardInfoResponse)
        }
        builder.endObject()
        return builder
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        // TODO: Modify this to have predictable fields
        if (::shardInfoResponse.isInitialized) {
            out.writeList(shardInfoResponse)
        }
        if (::status.isInitialized) {
            out.writeString(status)
        }
        if (::reason.isInitialized) {
            out.writeString(reason)
        }
        if (::connectionAlias.isInitialized) {
            out.writeString(connectionAlias)
        }
        if (::leaderIndexName.isInitialized) {
            out.writeString(leaderIndexName)
        }
        if (::followerIndexName.isInitialized) {
            out.writeString(followerIndexName)
        }
    }

    override fun toString(): String {
        return "ReplicationStatusResponse(shardInfoResponse=$shardInfoResponse, status='$status'," +
            " connectionAlias='$connectionAlias', leaderIndexName='$leaderIndexName', followerIndexName='$followerIndexName')"
    }
}
