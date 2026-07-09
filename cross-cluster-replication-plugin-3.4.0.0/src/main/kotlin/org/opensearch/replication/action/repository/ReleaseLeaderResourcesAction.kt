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

package org.opensearch.replication.action.repository

import org.opensearch.action.ActionType
import org.opensearch.action.support.clustermanager.AcknowledgedResponse

class ReleaseLeaderResourcesAction private constructor() : ActionType<AcknowledgedResponse>(NAME, ::AcknowledgedResponse)  {
    companion object {
        const val NAME = "indices:admin/plugins/replication/resources/release"
        val INSTANCE = ReleaseLeaderResourcesAction()
    }
}
