package org.opensearch.replication.action.autofollow

import org.opensearch.action.ActionType
import org.opensearch.action.support.master.AcknowledgedResponse

class AutoFollowMasterNodeAction: ActionType<AcknowledgedResponse>(NAME, ::AcknowledgedResponse) {
    companion object {
        const val NAME = "internal:cluster:admin/plugins/replication/autofollow/update"
        val INSTANCE = AutoFollowMasterNodeAction()
    }
}
