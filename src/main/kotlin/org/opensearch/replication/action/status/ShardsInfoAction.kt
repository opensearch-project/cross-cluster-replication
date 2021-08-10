package org.opensearch.replication.action.status

import org.opensearch.action.ActionType
import org.opensearch.common.io.stream.Writeable


class ShardsInfoAction : ActionType<ReplicationStatusResponse>(NAME, reader) {
    companion object {
        const val NAME = "indices:admin/plugins/replication/index/all-shards"
        val INSTANCE = ShardsInfoAction()
        val reader = Writeable.Reader { inp -> ReplicationStatusResponse(inp) }
    }

    override fun getResponseReader(): Writeable.Reader<ReplicationStatusResponse> = reader
}

