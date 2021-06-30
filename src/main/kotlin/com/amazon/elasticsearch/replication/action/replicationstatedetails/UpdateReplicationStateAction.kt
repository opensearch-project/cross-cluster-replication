package com.amazon.elasticsearch.replication.action.replicationstatedetails

import org.elasticsearch.action.ActionType
import org.elasticsearch.action.support.master.AcknowledgedResponse

class UpdateReplicationStateAction private constructor(): ActionType<AcknowledgedResponse>(NAME, ::AcknowledgedResponse) {
    companion object {
        const val NAME = "internal:cluster:admin/plugins/replication/index/state"
        val INSTANCE: UpdateReplicationStateAction = UpdateReplicationStateAction()
    }
}
