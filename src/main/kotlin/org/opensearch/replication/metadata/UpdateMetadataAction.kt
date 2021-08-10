/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

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