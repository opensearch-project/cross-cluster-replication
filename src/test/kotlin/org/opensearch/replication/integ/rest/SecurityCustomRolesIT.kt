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
import org.apache.hc.core5.http.HttpStatus
import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.assertj.core.api.Assertions
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.ResponseException
import org.opensearch.client.RestHighLevelClient
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.client.indices.GetIndexRequest
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.common.settings.Settings
import org.junit.Assert
import org.junit.Assume
import java.util.*
import java.util.concurrent.TimeUnit
import org.opensearch.replication.task.autofollow.AutoFollowExecutor
import org.opensearch.tasks.TaskInfo
import org.junit.Before
import org.opensearch.commons.replication.action.ReplicationActions.STOP_REPLICATION_ACTION_NAME

@MultiClusterAnnotations.ClusterConfigurations(
        MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
        MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)

class SecurityCustomRolesIT: SecurityBase()  {
    private val leaderIndexName = "leader_index"

    @Before
    fun beforeTest() {
        Assume.assumeTrue(isSecurityPropertyEnabled)
    }

    fun `test for FOLLOWER that START replication works for user with valid permissions`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()

        var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                useRoles = UseRoles(leaderClusterRole = "leaderRoleValidPerms",followerClusterRole = "followerRoleValidPerms"))

        followerClient.startReplication(startReplicationRequest,
                requestOptions= RequestOptions.DEFAULT.addBasicAuthHeader("testUser1",INTEG_TEST_PASSWORD), waitForRestore = true)
        assertBusy {
            Assertions.assertThat(followerClient.indices().exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT)).isEqualTo(true)
        }
    }

    fun `test for FOLLOWER that START replication is forbidden for user with invalid permissions`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()

        var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                useRoles = UseRoles(leaderClusterRole = "leaderRoleValidPerms",followerClusterRole = "followerRoleNoPerms"))

        Assertions.assertThatThrownBy { followerClient.startReplication(startReplicationRequest,
                requestOptions= RequestOptions.DEFAULT.addBasicAuthHeader("testUser2",INTEG_TEST_PASSWORD)) }
                    .isInstanceOf(ResponseException::class.java)
                    .hasMessageContaining("403 Forbidden")
    }

    fun `test for FOLLOWER that STOP replication works for user with valid permissions`() {
        val followerClient = getClientForCluster(FOLLOWER)

        Assertions.assertThatThrownBy {
            followerClient.stopReplication("follower-index1",
                    requestOptions= RequestOptions.DEFAULT.addBasicAuthHeader("testUser1",INTEG_TEST_PASSWORD))
        }.isInstanceOf(ResponseException::class.java)
                .hasMessageContaining("No replication in progress for index:follower-index1")
    }

    fun `test for FOLLOWER that STOP replication is forbidden for user with invalid permissions`() {
        val followerClient = getClientForCluster(FOLLOWER)

        Assertions.assertThatThrownBy {
            followerClient.stopReplication("follower-index1",
                    requestOptions= RequestOptions.DEFAULT.addBasicAuthHeader("testUser2",INTEG_TEST_PASSWORD))
        }.isInstanceOf(ResponseException::class.java)
        .hasMessageContaining("403 Forbidden")
    }

    fun `test for FOLLOWER that PAUSE replication works for user with valid permissions`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1-pause-valid"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()

        var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                useRoles = UseRoles(leaderClusterRole = "leaderRoleValidPerms",followerClusterRole = "followerRoleValidPerms"))
        var requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1",INTEG_TEST_PASSWORD)
        followerClient.startReplication(startReplicationRequest, waitForRestore = true,
                requestOptions = requestOptions)

        /* At this point, the follower cluster should be in FOLLOWING state. Next, we pause replication
        and verify the same
         */
        followerClient.pauseReplication(followerIndexName,
                requestOptions = requestOptions)

        // Validate paused replication using Status Api
        assertBusy {
            `validate aggregated paused status response`(followerClient.replicationStatus(followerIndexName,
                    requestOptions = requestOptions))
        }
    }

    fun `test for FOLLOWER that PAUSE replication is forbidden for user with invalid permissions`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()

        var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                useRoles = UseRoles(leaderClusterRole = "leaderRoleValidPerms",followerClusterRole = "followerRoleValidPerms"))

        followerClient.startReplication(startReplicationRequest, waitForRestore = true,
                requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1",INTEG_TEST_PASSWORD))

        Assertions.assertThatThrownBy {
            followerClient.pauseReplication(followerIndexName,
                    requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser2",INTEG_TEST_PASSWORD))
        }.isInstanceOf(ResponseException::class.java)
        .hasMessageContaining("403 Forbidden")
    }

    fun `test for FOLLOWER that STATUS Api works for user with valid permissions`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()

        var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                useRoles = UseRoles(leaderClusterRole = "leaderRoleValidPerms",followerClusterRole = "followerRoleValidPerms"))

        followerClient.startReplication(startReplicationRequest,  waitForRestore = true,
                requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1",INTEG_TEST_PASSWORD))

        assertBusy {
            `validate status syncing response`(followerClient.replicationStatus(followerIndexName,
                    requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1",INTEG_TEST_PASSWORD)))
        }
    }

    fun `test for FOLLOWER that STATUS Api is forbidden for user with invalid permissions`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()

        var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                useRoles = UseRoles(leaderClusterRole = "leaderRoleValidPerms",followerClusterRole = "followerRoleValidPerms"))

        followerClient.startReplication(startReplicationRequest, waitForRestore = true,
                requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1",INTEG_TEST_PASSWORD))

        Assertions.assertThatThrownBy {
            followerClient.replicationStatus(followerIndexName,
                    requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser2",INTEG_TEST_PASSWORD))
        }.isInstanceOf(ResponseException::class.java)
        .hasMessageContaining("403 Forbidden")
    }

    fun `test for FOLLOWER that UPDATE settings works for user with valid permissions`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1-settings-valid-perm"

        setMetadataSyncDelay()

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        var settings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .build()

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName).settings(settings), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()

        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName,
            useRoles = UseRoles(leaderClusterRole = "leaderRoleValidPerms",followerClusterRole = "followerRoleValidPerms")),
            requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1",INTEG_TEST_PASSWORD))
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
        followerClient.updateReplication(followerIndexName, settings,
                requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1",INTEG_TEST_PASSWORD))

        // Wait for the settings to get updated at follower cluster.
        assertBusy ({
            Assert.assertEquals(
                    "checksum",
                    followerClient.indices()
                            .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                            .indexToSettings.getOrDefault(followerIndexName, Settings.EMPTY)["index.shard.check_on_startup"]
            )
        }, 30L, TimeUnit.SECONDS)
    }

    fun `test for FOLLOWER that UPDATE settings is forbidden for user with invalid permissions`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1-settings-invalid-perm"
        setMetadataSyncDelay()
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        var settings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .build()
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName).settings(settings), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName,
                useRoles = UseRoles(leaderClusterRole = "leaderRoleValidPerms",followerClusterRole = "followerRoleValidPerms")),
                requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1",INTEG_TEST_PASSWORD), waitForRestore = true)
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
                    requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser2",INTEG_TEST_PASSWORD))
        }.isInstanceOf(ResponseException::class.java)
        .hasMessageContaining("403 Forbidden")
    }

    fun `test for FOLLOWER that AutoFollow works for user with valid permissions`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val indexPrefix = "follower-index1_"
        val connectionAlias = "test_conn"
        val indexPattern = "follower-index1*"
        val indexPatternName = "test_pattern"
        val leaderIndexName = createRandomIndex(indexPrefix, leaderClient)
        var leaderIndexNameNew = ""
        createConnectionBetweenClusters(FOLLOWER, LEADER, connectionAlias)
        try {
            followerClient.updateAutoFollowPattern(connectionAlias, indexPatternName, indexPattern,
                    useRoles = UseRoles(leaderClusterRole = "leaderRoleValidPerms",followerClusterRole = "followerRoleValidPerms"),
                    requestOptions= RequestOptions.DEFAULT.addBasicAuthHeader("testUser1",INTEG_TEST_PASSWORD))
            // Verify that existing index matching the pattern are replicated.
            assertBusy ({
                Assertions.assertThat(followerClient.indices()
                        .exists(GetIndexRequest(leaderIndexName), RequestOptions.DEFAULT))
                        .isEqualTo(true)
            }, 30, TimeUnit.SECONDS)
            Assertions.assertThat(getAutoFollowTasks(FOLLOWER).size).isEqualTo(1)
            leaderIndexNameNew = createRandomIndex(indexPrefix, leaderClient)
            // Verify that newly created index on leader which match the pattern are also replicated.
            assertBusy ({
                Assertions.assertThat(followerClient.indices()
                        .exists(GetIndexRequest(leaderIndexNameNew), RequestOptions.DEFAULT))
                        .isEqualTo(true)
            }, 60, TimeUnit.SECONDS)
        } finally {
            followerClient.deleteAutoFollowPattern(connectionAlias, indexPatternName)
        }
    }

    fun `test for FOLLOWER that AutoFollow is forbidden for user with invalid permissions`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val connectionAlias = "test_conn"
        val indexPattern = "follower-index1*"
        val indexPatternName = "test_pattern"
        createConnectionBetweenClusters(FOLLOWER, LEADER, connectionAlias)
        Assertions.assertThatThrownBy {
            followerClient.updateAutoFollowPattern(connectionAlias, indexPatternName, indexPattern,
                    useRoles = UseRoles(leaderClusterRole = "leaderRoleValidPerms",followerClusterRole = "followerRoleNoPerms"),
                    requestOptions= RequestOptions.DEFAULT.addBasicAuthHeader("testUser2",INTEG_TEST_PASSWORD))
        }.isInstanceOf(ResponseException::class.java)
        .hasMessageContaining("403 Forbidden")
    }

    private fun createRandomIndex(indexPrefix : String, client: RestHighLevelClient): String {
        val indexName = indexPrefix + randomAlphaOfLength(6).toLowerCase(Locale.ROOT)
        val createIndexResponse = client.indices().create(CreateIndexRequest(indexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        assertBusy {
            Assertions.assertThat(client.indices()
                    .exists(GetIndexRequest(indexName), RequestOptions.DEFAULT))
                    .isEqualTo(true)
        }
        return indexName
    }

    private fun getAutoFollowTasks(clusterName: String): List<TaskInfo> {
        return getReplicationTaskList(clusterName, AutoFollowExecutor.TASK_NAME + "*")
    }

    fun `test for FOLLOWER that REVOKE replay permission`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1"
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                    useRoles = UseRoles(leaderClusterRole = "leaderRoleValidPerms",followerClusterRole = "followerRoleValidPerms"))
            followerClient.startReplication(startReplicationRequest, waitForRestore = true,
                    requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1",INTEG_TEST_PASSWORD))
            insertDocToIndex(LEADER, "1", "dummy data 1",leaderIndexName)
            //Querying ES cluster throws random exceptions like ClusterManagerNotDiscovered or ShardsFailed etc, so catching them and retrying
            assertBusy ({
                try {
                    Assertions.assertThat(docs(FOLLOWER, followerIndexName)).contains("dummy data 1")
                } catch (ex: Exception) {
                    Assert.fail("Exception while querying follower cluster. Failing to retry again")
                }
            }, 1, TimeUnit.MINUTES)
            assertBusy {
                `validate status syncing response`(followerClient.replicationStatus(followerIndexName,
                        requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1",INTEG_TEST_PASSWORD)))
            }

            updateRole(followerIndexName,"followerRoleValidPerms", false)
            insertDocToIndex(LEADER, "2", "dummy data 2",leaderIndexName)

            assertBusy ({
                validatePausedState(followerClient.replicationStatus(followerIndexName,
                        requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1",INTEG_TEST_PASSWORD)))
            }, 100, TimeUnit.SECONDS)
        } finally {
            updateRole(followerIndexName,"followerRoleValidPerms", true)
        }
    }

    private fun validatePausedState(statusResp: Map<String, Any>) {
        Assert.assertEquals(statusResp.getValue("status"),"PAUSED")
        Assert.assertTrue((statusResp.getValue("reason")).toString().contains("no permissions for [indices:data/write/plugins/replication/changes] and associated roles"))
        Assert.assertFalse(statusResp.containsKey("shard_replication_details"))
        Assert.assertFalse(statusResp.containsKey("local_checkpoint"))
        Assert.assertFalse(statusResp.containsKey("remote_checkpoint"))
    }

    private fun updateRole(indexPattern: String, role: String, shouldAddWritePermission: Boolean) {
        val followerClient = testClusters.get(FOLLOWER)
        val persistentConnectionRequest = Request("PUT", "_plugins/_security/api/roles/"+role)
        var writePermission : String = ""
        if(shouldAddWritePermission)
            writePermission = "\"indices:data/write/plugins/replication/changes\","
        val entityAsString = """
            {
                "cluster_permissions": [
                    "cluster:admin/plugins/replication/autofollow/update"
                ],
                "index_permissions": [
                    {
                        "index_patterns": ["$indexPattern*"],
                        "allowed_actions": [
                            "indices:admin/plugins/replication/index/setup/validate",
                            $writePermission
                            "indices:admin/plugins/replication/index/start",
                            "indices:admin/plugins/replication/index/pause",
                            "indices:admin/plugins/replication/index/resume",
                            "$STOP_REPLICATION_ACTION_NAME",
                            "indices:admin/plugins/replication/index/update",
                            "indices:admin/plugins/replication/index/status_check"
                        ]
                    }
                ]
            }
            """.trimMargin()
        persistentConnectionRequest.entity = StringEntity(entityAsString, ContentType.APPLICATION_JSON)
        val persistentConnectionResponse = followerClient!!.lowLevelClient.performRequest(persistentConnectionRequest)
        assertEquals(HttpStatus.SC_OK.toLong(), persistentConnectionResponse.statusLine.statusCode.toLong())
    }
}
