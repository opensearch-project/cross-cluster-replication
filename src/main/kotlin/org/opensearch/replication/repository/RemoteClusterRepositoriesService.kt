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

package org.opensearch.replication.repository
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.repositories.RepositoriesService
import org.opensearch.transport.SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS
import java.util.function.Supplier

class RemoteClusterRepositoriesService(private val repositoriesService: Supplier<RepositoriesService>,
                                       clusterService: ClusterService) {

    init {
        listenForUpdates(clusterService.clusterSettings)
    }

    private fun listenForUpdates(clusterSettings: ClusterSettings) {
        // TODO: Proxy support from ES 7.7. Needs additional handling based on those settings
        clusterSettings.addAffixUpdateConsumer(REMOTE_CLUSTER_SEEDS, this::updateRepositoryDetails) { _, _ -> Unit }
    }

    private fun updateRepositoryDetails(alias: String, seeds: List<String>?) {
        if(seeds == null || seeds.isEmpty()) {
            repositoriesService.get().unregisterInternalRepository(REMOTE_REPOSITORY_PREFIX + alias)
            return
        }
        //TODO: Check to see if register should happen based on every seed node update
        repositoriesService.get().registerInternalRepository(REMOTE_REPOSITORY_PREFIX + alias, REMOTE_REPOSITORY_TYPE)
    }

}