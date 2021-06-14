package com.amazon.elasticsearch.replication.action.setup

import org.elasticsearch.action.ActionType
import org.elasticsearch.action.support.master.AcknowledgedResponse

class SetupChecksAction private constructor(): ActionType<AcknowledgedResponse>(NAME, ::AcknowledgedResponse) {
    companion object {
        const val NAME = "internal:indices/admin/opendistro/replication/index/setup"
        val INSTANCE: SetupChecksAction = SetupChecksAction()
    }
}
