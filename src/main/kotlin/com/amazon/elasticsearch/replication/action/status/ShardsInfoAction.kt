package com.amazon.elasticsearch.replication.action.status

import org.elasticsearch.action.ActionType
import org.elasticsearch.common.io.stream.Writeable


class ShardsInfoAction : ActionType<ReplicationStatusResponse>(NAME, reader) {
    companion object {
        const val NAME = "indices:admin/opendistro/replication/index/all-shards"
        val INSTANCE = ShardsInfoAction()
        val reader = Writeable.Reader { inp -> ReplicationStatusResponse(inp) }
    }

    override fun getResponseReader(): Writeable.Reader<ReplicationStatusResponse> = reader
}

