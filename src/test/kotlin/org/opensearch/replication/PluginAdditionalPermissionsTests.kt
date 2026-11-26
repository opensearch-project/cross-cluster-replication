/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.replication

import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.yaml.YamlXContent
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.test.OpenSearchTestCase

/** Checks that the permissions file grants the cluster classified actions the metadata store sends as the plugin. */
class PluginAdditionalPermissionsTests : OpenSearchTestCase() {

    fun testTheFileGrantsTheStoreHealthWaitAndMultiGet() {
        val resource = ReplicationPlugin::class.java.classLoader.getResource("plugin-additional-permissions.yml")
        assertNotNull("plugin-additional-permissions.yml is packaged with the plugin", resource)
        val permissions = YamlXContent.yamlXContent
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, resource!!.readText())
            .use { it.map() }

        assertEquals(
            setOf("cluster:monitor/health", "indices:data/read/mget"),
            (permissions["cluster_permissions"] as List<*>).toSet()
        )
        assertNull("index actions on the registered index need no grant", permissions["index_permissions"])
    }
}
