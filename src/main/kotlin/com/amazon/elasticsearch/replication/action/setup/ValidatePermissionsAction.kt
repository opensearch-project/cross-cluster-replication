package com.amazon.elasticsearch.replication.action.setup

import org.elasticsearch.action.ActionType

class ValidatePermissionsAction private constructor(): ActionType<ValidatePermissionsResponse>(NAME, ::ValidatePermissionsResponse){
    companion object {
        const val NAME = "indices:admin/opendistro/replication/index/setup/validate"
        val INSTANCE: ValidatePermissionsAction = ValidatePermissionsAction()
    }
}
