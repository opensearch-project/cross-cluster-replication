/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
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