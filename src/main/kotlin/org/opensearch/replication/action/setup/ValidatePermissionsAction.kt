package org.opensearch.replication.action.setup

import org.opensearch.action.ActionType
import org.opensearch.action.support.master.AcknowledgedResponse

class ValidatePermissionsAction private constructor(): ActionType<AcknowledgedResponse>(NAME, ::AcknowledgedResponse){
    companion object {
        const val NAME = "indices:admin/plugins/replication/index/setup/validate"
        val INSTANCE: ValidatePermissionsAction = ValidatePermissionsAction()
    }
}
