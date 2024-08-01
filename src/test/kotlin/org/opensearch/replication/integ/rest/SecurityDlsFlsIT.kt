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

package org.opensearch.replication.integ.rest

import org.opensearch.replication.*
import org.opensearch.replication.util.addBasicAuthHeader
import org.assertj.core.api.Assertions
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest
import org.opensearch.client.RequestOptions
import org.opensearch.client.ResponseException
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.client.indices.GetIndexRequest
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.common.settings.Settings
import org.opensearch.test.OpenSearchTestCase
import org.junit.Assert
import org.junit.Assume
import org.junit.Before

@MultiClusterAnnotations.ClusterConfigurations(
        MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER, forceInitSecurityConfiguration = true),
        MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER, forceInitSecurityConfiguration = true)
)
class SecurityDlsFlsIT: SecurityBase() {
    private val leaderIndexName = "leader_index"
    private val DLS_FLS_EXCEPTION_MESSAGE = "Cross Cluster Replication is not supported when FLS or DLS or Fieldmasking is activated"

    @Before
    fun beforeTest() {
        Assume.assumeTrue(isSecurityPropertyEnabled)
    }

    fun `test for FOLLOWER that START replication is forbidden for user with DLS or FLS enabled`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1-dlsfls-enabled"
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                    useRoles = UseRoles(leaderClusterRole = "leaderRoleValidPerms",followerClusterRole = "followerDlsRole"))
        Assertions.assertThatThrownBy { followerClient.startReplication(startReplicationRequest,
                    requestOptions= RequestOptions.DEFAULT.addBasicAuthHeader("testUser3",INTEG_TEST_PASSWORD)) }
                .isInstanceOf(ResponseException::class.java)
                .hasMessageContaining(DLS_FLS_EXCEPTION_MESSAGE)
                .hasMessageContaining("403 Forbidden")
    }

    fun `test for FOLLOWER that STOP replication is forbidden for user with DLS or FLS enabled`() {
        val followerClient = getClientForCluster(FOLLOWER)
        Assertions.assertThatThrownBy {
            followerClient.stopReplication("follower-index1-stop-forbidden",
                    requestOptions= RequestOptions.DEFAULT.addBasicAuthHeader("testUser3",INTEG_TEST_PASSWORD))
        }.isInstanceOf(ResponseException::class.java)
        .hasMessageContaining(DLS_FLS_EXCEPTION_MESSAGE)
        .hasMessageContaining("403 Forbidden")
    }

    fun `test for FOLLOWER that PAUSE replication is forbidden for user with DLS or FLS enabled`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1-pause-forbidden"
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                useRoles = UseRoles(leaderClusterRole = "leaderRoleValidPerms",followerClusterRole = "followerRoleValidPerms"))
        followerClient.startReplication(startReplicationRequest, waitForRestore = true,
                requestOptions= RequestOptions.DEFAULT.addBasicAuthHeader("testUser1",INTEG_TEST_PASSWORD))
        Assertions.assertThatThrownBy {
            followerClient.pauseReplication(followerIndexName,
                    requestOptions= RequestOptions.DEFAULT.addBasicAuthHeader("testUser3",INTEG_TEST_PASSWORD))
        }.isInstanceOf(ResponseException::class.java)
        .hasMessageContaining(DLS_FLS_EXCEPTION_MESSAGE)
        .hasMessageContaining("403 Forbidden")
    }

    fun `test for FOLLOWER that STATUS Api is forbidden for user with DLS or FLS enabled`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1-status-forbidden"
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                useRoles = UseRoles(leaderClusterRole = "leaderRoleValidPerms",followerClusterRole = "followerRoleValidPerms"))
        followerClient.startReplication(startReplicationRequest, waitForRestore = true,
                requestOptions= RequestOptions.DEFAULT.addBasicAuthHeader("testUser1",INTEG_TEST_PASSWORD))
        Assertions.assertThatThrownBy {
            followerClient.replicationStatus(followerIndexName,
                    requestOptions= RequestOptions.DEFAULT.addBasicAuthHeader("testUser3",INTEG_TEST_PASSWORD))
        }.isInstanceOf(ResponseException::class.java)
        .hasMessageContaining(DLS_FLS_EXCEPTION_MESSAGE)
        .hasMessageContaining("403 Forbidden")
    }

    fun `test for FOLLOWER that UPDATE settings is forbidden for user with DLS or FLS enabled`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1-update-forbidden"
        setMetadataSyncDelay()
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        var settings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .build()
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName).settings(settings), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName,
                useRoles = UseRoles(leaderClusterRole = "leaderRoleValidPerms",followerClusterRole = "followerRoleValidPerms")),
                requestOptions= RequestOptions.DEFAULT.addBasicAuthHeader("testUser1",INTEG_TEST_PASSWORD), waitForRestore = true)
        assertBusy {
            Assertions.assertThat(followerClient.indices()
                    .exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT))
                    .isEqualTo(true)
        }
        val getSettingsRequest = GetSettingsRequest()
        getSettingsRequest.indices(followerIndexName)
        Assert.assertEquals(
                "1",
                followerClient.indices()
                        .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                        .indexToSettings.getOrDefault(followerIndexName, Settings.EMPTY)[IndexMetadata.SETTING_NUMBER_OF_REPLICAS]
        )
        settings = Settings.builder()
                .put("index.shard.check_on_startup", "checksum")
                .build()
        Assertions.assertThatThrownBy {
            followerClient.updateReplication(followerIndexName, settings,
                    requestOptions= RequestOptions.DEFAULT.addBasicAuthHeader("testUser3",INTEG_TEST_PASSWORD))
        }.isInstanceOf(ResponseException::class.java)
        .hasMessageContaining(DLS_FLS_EXCEPTION_MESSAGE)
        .hasMessageContaining("403 Forbidden")
    }

    fun `test for FOLLOWER that START replication is forbidden for user with FLS enabled`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1-start-forbidden"
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                useRoles = UseRoles(leaderClusterRole = "leaderRoleValidPerms",followerClusterRole = "followerFlsRole"))
        Assertions.assertThatThrownBy { followerClient.startReplication(startReplicationRequest,
                requestOptions= RequestOptions.DEFAULT.addBasicAuthHeader("testUser4",INTEG_TEST_PASSWORD)) }
        .isInstanceOf(ResponseException::class.java)
        .hasMessageContaining(DLS_FLS_EXCEPTION_MESSAGE)
        .hasMessageContaining("403 Forbidden")
    }

    fun `test for FOLLOWER that START replication is forbidden for user with Field Masking enabled`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1-start-only-fls"
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                useRoles = UseRoles(leaderClusterRole = "leaderRoleValidPerms",followerClusterRole = "followerFieldMaskRole"))
        Assertions.assertThatThrownBy { followerClient.startReplication(startReplicationRequest,
                requestOptions= RequestOptions.DEFAULT.addBasicAuthHeader("testUser5",INTEG_TEST_PASSWORD)) }
        .isInstanceOf(ResponseException::class.java)
        .hasMessageContaining(DLS_FLS_EXCEPTION_MESSAGE)
        .hasMessageContaining("403 Forbidden")
    }

    fun `test for FOLLOWER that START replication works for user with Field Masking enabled on a different index pattern`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1-allow-start"
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse =
            leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        var startReplicationRequest = StartReplicationRequest(
            "source", leaderIndexName, followerIndexName,
            useRoles = UseRoles(
                leaderClusterRole = "leaderRoleValidPerms",
                followerClusterRole = "followerFieldMaskRole2"
            )
        )
        followerClient.startReplication(
            startReplicationRequest,
            requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser7", INTEG_TEST_PASSWORD),
            waitForRestore = true
        )
        OpenSearchTestCase.assertBusy {
            Assertions.assertThat(
                followerClient.indices().exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT)
            ).isEqualTo(true)
        }
    }
}