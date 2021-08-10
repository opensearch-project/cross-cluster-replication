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

package org.opensearch.replication.action.setup

import org.opensearch.action.ActionType
import org.opensearch.action.support.master.AcknowledgedResponse

class SetupChecksAction private constructor(): ActionType<AcknowledgedResponse>(NAME, ::AcknowledgedResponse) {
    companion object {
        const val NAME = "internal:indices/admin/plugins/replication/index/setup"
        val INSTANCE: SetupChecksAction = SetupChecksAction()
    }
}
