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

package org.opensearch.replication.repository
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.repositories.RepositoriesService
import org.opensearch.transport.ProxyConnectionStrategy.PROXY_ADDRESS
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
        clusterSettings.addAffixUpdateConsumer(PROXY_ADDRESS, this::updateRepositoryDetails) { _, _ -> Unit }
    }

    private fun updateRepositoryDetails(alias: String, seeds: List<String>?) {
        if(seeds.isNullOrEmpty()) {
            repositoriesService.get().unregisterInternalRepository(REMOTE_REPOSITORY_PREFIX + alias)
            return
        }
        //TODO: Check to see if register should happen based on every seed node update
        repositoriesService.get().registerInternalRepository(REMOTE_REPOSITORY_PREFIX + alias, REMOTE_REPOSITORY_TYPE)
    }

    private fun updateRepositoryDetails(alias: String, proxyIp: String?) {
        if(proxyIp.isNullOrEmpty()) {
            repositoriesService.get().unregisterInternalRepository(REMOTE_REPOSITORY_PREFIX + alias)
            return
        }
        repositoriesService.get().registerInternalRepository(REMOTE_REPOSITORY_PREFIX + alias, REMOTE_REPOSITORY_TYPE)
    }
}