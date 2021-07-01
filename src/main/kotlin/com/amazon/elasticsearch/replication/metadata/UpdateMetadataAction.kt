package com.amazon.elasticsearch.replication.metadata

import org.elasticsearch.action.ActionType
import org.elasticsearch.action.support.master.AcknowledgedResponse


class UpdateMetadataAction private constructor(): ActionType<AcknowledgedResponse>(
    NAME, ::AcknowledgedResponse) {
    companion object {
            const val NAME = "indices:admin/plugins/replication/index/update_metadata"
            val INSTANCE: UpdateMetadataAction = UpdateMetadataAction()
    }
}