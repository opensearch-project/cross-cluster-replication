package org.opensearch.replication.action.replicationstatedetails

import org.opensearch.action.ActionType
import org.opensearch.action.support.master.AcknowledgedResponse

class UpdateReplicationStateAction private constructor(): ActionType<AcknowledgedResponse>(NAME, ::AcknowledgedResponse) {
    companion object {
        const val NAME = "internal:cluster:admin/plugins/replication/index/state"
        val INSTANCE: UpdateReplicationStateAction = UpdateReplicationStateAction()
    }
}
