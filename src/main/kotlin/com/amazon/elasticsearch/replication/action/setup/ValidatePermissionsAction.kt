package com.amazon.elasticsearch.replication.action.setup

import org.elasticsearch.action.ActionType
import org.elasticsearch.action.support.master.AcknowledgedResponse

class ValidatePermissionsAction private constructor(): ActionType<AcknowledgedResponse>(NAME, ::AcknowledgedResponse){
    companion object {
        const val NAME = "indices:admin/opendistro/replication/index/setup/validate"
        val INSTANCE: ValidatePermissionsAction = ValidatePermissionsAction()
    }
}
