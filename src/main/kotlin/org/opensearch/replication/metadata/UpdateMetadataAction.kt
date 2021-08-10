package org.opensearch.replication.metadata

import org.opensearch.action.ActionType
import org.opensearch.action.support.master.AcknowledgedResponse


class UpdateMetadataAction private constructor(): ActionType<AcknowledgedResponse>(
    NAME, ::AcknowledgedResponse) {
    companion object {
            const val NAME = "indices:admin/plugins/replication/index/update_metadata"
            val INSTANCE: UpdateMetadataAction = UpdateMetadataAction()
    }
}