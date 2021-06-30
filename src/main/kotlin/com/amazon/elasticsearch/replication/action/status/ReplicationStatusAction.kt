package com.amazon.elasticsearch.replication.action.status

import org.elasticsearch.action.ActionType
import org.elasticsearch.common.io.stream.Writeable

class ReplicationStatusAction : ActionType<ReplicationStatusResponse>(NAME, reader) {
    companion object {
        const val NAME = "indices:admin/plugins/replication/index/status-check"
        val INSTANCE = ReplicationStatusAction()
        val reader = Writeable.Reader { inp -> ReplicationStatusResponse(inp) }
    }

    override fun getResponseReader(): Writeable.Reader<ReplicationStatusResponse> = reader
}
