package com.amazon.elasticsearch.replication.action.checkpoint


import org.elasticsearch.action.ActionType
import org.elasticsearch.common.io.stream.Writeable

class FetchGlobalCheckPointAction : ActionType<RemoteGlobalCheckPointResponse>(NAME, reader) {
    companion object {
        const val NAME = "indices:admin/opendistro/replication/index/getremotecheckpoint"
        val INSTANCE = FetchGlobalCheckPointAction()
        val reader = Writeable.Reader { inp -> RemoteGlobalCheckPointResponse(inp) }
    }

    override fun getResponseReader(): Writeable.Reader<RemoteGlobalCheckPointResponse> = reader
}