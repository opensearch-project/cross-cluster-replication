package com.amazon.elasticsearch.replication.action.autofollow

import org.elasticsearch.action.ActionType
import org.elasticsearch.action.support.master.AcknowledgedResponse

class AutoFollowMasterNodeAction: ActionType<AcknowledgedResponse>(NAME, ::AcknowledgedResponse) {
    companion object {
        const val NAME = "internal:cluster:admin/opendistro/replication/autofollow/update"
        val INSTANCE = AutoFollowMasterNodeAction()
    }
}
