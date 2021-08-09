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

package org.opensearch.replication.util

import org.opensearch.common.component.AbstractLifecycleComponent
import org.opensearch.common.inject.Inject
import org.opensearch.indices.IndicesService
import org.opensearch.persistent.PersistentTasksService

lateinit var indicesService: IndicesService
lateinit var persistentTasksService: PersistentTasksService

/**
 * Provides access to services and components that are not directly available via the [Plugin] interface. This class
 * simply get the required instances via the injector and saves them to static variables for access elsewhere.
 */
class Injectables @Inject constructor(indicesSvc: IndicesService,
                                      persistentTasksSvc: PersistentTasksService)
    : AbstractLifecycleComponent() {

    init {
        indicesService = indicesSvc
        persistentTasksService = persistentTasksSvc
    }

    override fun doStart() {
    }

    override fun doStop() {
    }

    override fun doClose() {
    }
}