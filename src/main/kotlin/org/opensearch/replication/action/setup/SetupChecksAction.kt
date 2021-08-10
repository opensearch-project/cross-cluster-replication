package org.opensearch.replication.action.setup

import org.opensearch.action.ActionType
import org.opensearch.action.support.master.AcknowledgedResponse

class SetupChecksAction private constructor(): ActionType<AcknowledgedResponse>(NAME, ::AcknowledgedResponse) {
    companion object {
        const val NAME = "internal:indices/admin/plugins/replication/index/setup"
        val INSTANCE: SetupChecksAction = SetupChecksAction()
    }
}
