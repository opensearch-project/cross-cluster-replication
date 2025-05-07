/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.replication.action.replay

import org.opensearch.action.ActionType

class ReplayChangesAction private constructor() : ActionType<ReplayChangesResponse>(NAME, ::ReplayChangesResponse) {

    companion object {
        const val NAME = "indices:data/write/plugins/replication/changes"
        val INSTANCE = ReplayChangesAction()
    }
}
