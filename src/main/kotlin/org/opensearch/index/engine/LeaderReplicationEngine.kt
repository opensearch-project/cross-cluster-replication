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

package org.opensearch.index.engine

import org.opensearch.index.translog.ReplicationTranslogDeletionPolicy
import org.opensearch.index.translog.TranslogDeletionPolicy

class LeaderReplicationEngine(config: EngineConfig) : InternalEngine(config) {

    override fun getTranslogDeletionPolicy(engineConfig: EngineConfig): TranslogDeletionPolicy {
        return ReplicationTranslogDeletionPolicy(
            engineConfig.indexSettings,
            engineConfig.retentionLeasesSupplier()
        )
    }
}
