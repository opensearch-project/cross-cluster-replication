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

package org.opensearch.replication

import org.junit.After
import org.junit.Before
import org.junit.Test
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.core.common.unit.ByteSizeUnit
import org.opensearch.core.common.unit.ByteSizeValue
import org.opensearch.test.ClusterServiceUtils
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.TestThreadPool
import java.util.concurrent.TimeUnit

class ReplicationSettingsTests : OpenSearchTestCase() {

    private lateinit var threadPool: TestThreadPool
    private lateinit var clusterService: ClusterService
    private lateinit var clusterSettings: ClusterSettings
    private lateinit var replicationSettings: ReplicationSettings

    @Before
    fun setup() {
        threadPool = TestThreadPool("ReplicationSettingsTests")

        clusterSettings = ClusterSettings(
            Settings.EMPTY,
            ReplicationPlugin().settings.filter { it.properties.contains(Setting.Property.NodeScope) }.toSet()
        )

        val clusterState = ClusterState.builder(ClusterName.DEFAULT).build()
        clusterService = ClusterServiceUtils.createClusterService(clusterState, threadPool)

        val clusterServiceField = clusterService.javaClass.getDeclaredField("clusterSettings")
        clusterServiceField.isAccessible = true
        clusterServiceField.set(clusterService, clusterSettings)

        replicationSettings = ReplicationSettings(clusterService)
    }

    @After
    fun cleanup() {
        clusterService.close()
        threadPool.shutdown()
        if (!threadPool.awaitTermination(5, TimeUnit.SECONDS)) {
            threadPool.shutdownNow()
        }
    }

    @Test
    fun `test leader translog generation threshold size defaults to 32mb`() {
        assertEquals(ByteSizeValue(32, ByteSizeUnit.MB), replicationSettings.leaderTranslogGenerationThresholdSize)
        assertEquals(ByteSizeValue(32, ByteSizeUnit.MB),
            ReplicationPlugin.REPLICATION_FOLLOWER_LEADER_TRANSLOG_GENERATION_THRESHOLD_SIZE.getDefault(Settings.EMPTY))
    }

    @Test
    fun `test leader translog generation threshold size is dynamically updatable`() {
        clusterSettings.applySettings(Settings.builder()
            .put(ReplicationPlugin.REPLICATION_FOLLOWER_LEADER_TRANSLOG_GENERATION_THRESHOLD_SIZE.key, "2mb")
            .build())
        assertEquals(ByteSizeValue(2, ByteSizeUnit.MB), replicationSettings.leaderTranslogGenerationThresholdSize)

        clusterSettings.applySettings(Settings.EMPTY)
        assertEquals(ByteSizeValue(32, ByteSizeUnit.MB), replicationSettings.leaderTranslogGenerationThresholdSize)
    }

    @Test
    fun `test leader translog generation threshold size rejects values outside its bounds`() {
        val setting = ReplicationPlugin.REPLICATION_FOLLOWER_LEADER_TRANSLOG_GENERATION_THRESHOLD_SIZE
        assertEquals(ByteSizeValue(1, ByteSizeUnit.MB), setting.get(Settings.builder().put(setting.key, "1mb").build()))
        assertEquals(ByteSizeValue(1, ByteSizeUnit.GB), setting.get(Settings.builder().put(setting.key, "1gb").build()))
        expectThrows(IllegalArgumentException::class.java) {
            setting.get(Settings.builder().put(setting.key, "512kb").build())
        }
        expectThrows(IllegalArgumentException::class.java) {
            setting.get(Settings.builder().put(setting.key, "2gb").build())
        }
    }
}
