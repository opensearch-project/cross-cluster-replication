package com.amazon.elasticsearch.replication.action.status

import org.elasticsearch.action.ActionType
import org.elasticsearch.action.support.master.AcknowledgedResponse

class IndexReplicationStatusAction private constructor(): ActionType<StatusResponse>(NAME, ::StatusResponse) {
    companion object {
        const val NAME = "indices:admin/opendistro/replication/index/status-check"
        val INSTANCE: IndexReplicationStatusAction = IndexReplicationStatusAction()
    }
}