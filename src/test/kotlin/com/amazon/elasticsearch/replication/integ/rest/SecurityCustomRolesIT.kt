package com.amazon.elasticsearch.replication.integ.rest

import com.amazon.elasticsearch.replication.*
import com.amazon.elasticsearch.replication.util.addBasicAuthHeader
import org.apache.http.HttpStatus
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.assertj.core.api.Assertions
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest
import org.elasticsearch.client.Request
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.ResponseException
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.test.ESTestCase
import org.junit.Assert
import org.junit.Assume
import java.util.*
import java.util.concurrent.TimeUnit
import com.amazon.elasticsearch.replication.task.autofollow.AutoFollowExecutor
import org.elasticsearch.tasks.TaskInfo
import org.junit.Before

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
        try {
            var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                    useRoles = UseRoles(leaderClusterRole = "leaderRoleValidPerms",followerClusterRole = "followerRoleValidPerms"))

            followerClient.startReplication(startReplicationRequest,
                    requestOptions= RequestOptions.DEFAULT.addBasicAuthHeader("testUser1","password"))
            ESTestCase.assertBusy {
                Assertions.assertThat(followerClient.indices().exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT)).isEqualTo(true)
            }
        } finally {
            followerClient.stopReplication(followerIndexName)
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
                requestOptions= RequestOptions.DEFAULT.addBasicAuthHeader("testUser2","password")) }
                    .isInstanceOf(ResponseException::class.java)
                    .hasMessageContaining("403 Forbidden")
    }

    fun `test for FOLLOWER that STOP replication works for user with valid permissions`() {
        val followerClient = getClientForCluster(FOLLOWER)

        Assertions.assertThatThrownBy {
            followerClient.stopReplication("follower-index1",
                    requestOptions= RequestOptions.DEFAULT.addBasicAuthHeader("testUser1","password"))
        }.isInstanceOf(ResponseException::class.java)
                .hasMessageContaining("No replication in progress for index:follower-index1")
    }

    fun `test for FOLLOWER that STOP replication is forbidden for user with invalid permissions`() {
        val followerClient = getClientForCluster(FOLLOWER)

        Assertions.assertThatThrownBy {
            followerClient.stopReplication("follower-index1",
                    requestOptions= RequestOptions.DEFAULT.addBasicAuthHeader("testUser2","password"))
        }.isInstanceOf(ResponseException::class.java)
        .hasMessageContaining("403 Forbidden")
    }

    fun `test for FOLLOWER that PAUSE replication works for user with valid permissions`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                    useRoles = UseRoles(leaderClusterRole = "leaderRoleValidPerms",followerClusterRole = "followerRoleValidPerms"))
            var requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1","password")
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
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test for FOLLOWER that PAUSE replication is forbidden for user with invalid permissions`() {
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
                    requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1","password"))

            Assertions.assertThatThrownBy {
                followerClient.pauseReplication(followerIndexName,
                        requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser2","password"))
            }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining("403 Forbidden")

        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test for FOLLOWER that STATUS Api works for user with valid permissions`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                    useRoles = UseRoles(leaderClusterRole = "leaderRoleValidPerms",followerClusterRole = "followerRoleValidPerms"))

            followerClient.startReplication(startReplicationRequest,  waitForRestore = true,
                    requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1","password"))

            assertBusy {
                `validate status syncing response`(followerClient.replicationStatus(followerIndexName,
                        requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1","password")))
            }
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test for FOLLOWER that STATUS Api is forbidden for user with invalid permissions`() {
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
                    requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1","password"))

            Assertions.assertThatThrownBy {
                followerClient.replicationStatus(followerIndexName,
                        requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser2","password"))
            }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining("403 Forbidden")
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test for FOLLOWER that UPDATE settings works for user with valid permissions`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1"

        setMetadataSyncDelay()

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        var settings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .build()

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName).settings(settings), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName,
                useRoles = UseRoles(leaderClusterRole = "leaderRoleValidPerms",followerClusterRole = "followerRoleValidPerms")),
                requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1","password"))
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
                            .indexToSettings[followerIndexName][IndexMetadata.SETTING_NUMBER_OF_REPLICAS]
            )

            settings = Settings.builder()
                    .put("index.shard.check_on_startup", "checksum")
                    .build()
            followerClient.updateReplication(followerIndexName, settings,
                    requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1","password"))

            assertBusy {
                Assert.assertEquals(
                        "checksum",
                        followerClient.indices()
                                .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                                .indexToSettings[followerIndexName]["index.shard.check_on_startup"]
                )
            }
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test for FOLLOWER that UPDATE settings is forbidden for user with invalid permissions`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1"

        setMetadataSyncDelay()

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        var settings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .build()

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName).settings(settings), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName,
                    useRoles = UseRoles(leaderClusterRole = "leaderRoleValidPerms",followerClusterRole = "followerRoleValidPerms")),
                    requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1","password"))
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
                            .indexToSettings[followerIndexName][IndexMetadata.SETTING_NUMBER_OF_REPLICAS]
            )

            settings = Settings.builder()
                    .put("index.shard.check_on_startup", "checksum")
                    .build()

            Assertions.assertThatThrownBy {
                followerClient.updateReplication(followerIndexName, settings,
                        requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser2","password"))
            }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining("403 Forbidden")
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
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
                    requestOptions= RequestOptions.DEFAULT.addBasicAuthHeader("testUser1","password"))

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
            followerClient.stopReplication(leaderIndexName, false)
            followerClient.stopReplication(leaderIndexNameNew)
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
                    requestOptions= RequestOptions.DEFAULT.addBasicAuthHeader("testUser2","password"))
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
                    requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1","password"))

            insertDocToIndex(LEADER, "1", "dummy data 1",leaderIndexName)
            //Querying ES cluster throws random exceptions like MasterNotDiscovered or ShardsFailed etc, so catching them and retrying
            assertBusy ({
                try {
                    Assertions.assertThat(docs(FOLLOWER, followerIndexName)).contains("dummy data 1")
                } catch (ex: Exception) {
                    Assert.fail("Exception while querying follower cluster. Failing to retry again")
                }
            }, 1, TimeUnit.MINUTES)

            assertBusy {
                `validate status syncing response`(followerClient.replicationStatus(followerIndexName,
                        requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1","password")))
            }

            updateRole(followerIndexName,"followerRoleValidPerms", false)
            insertDocToIndex(LEADER, "2", "dummy data 2",leaderIndexName)

            assertBusy ({
                validatePausedState(followerClient.replicationStatus(followerIndexName,
                        requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1","password")))
            }, 100, TimeUnit.SECONDS)
        } finally {
            updateRole(followerIndexName,"followerRoleValidPerms", true)
            followerClient.stopReplication(followerIndexName)
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
        val persistentConnectionRequest = Request("PUT", "_opendistro/_security/api/roles/"+role)
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
                            "indices:admin/plugins/replication/index/stop",
                            "indices:admin/plugins/replication/index/update",
                            "indices:admin/plugins/replication/index/status_check"
                        ]
                    }
                ]
            }
            """.trimMargin()
        persistentConnectionRequest.entity = NStringEntity(entityAsString, ContentType.APPLICATION_JSON)
        val persistentConnectionResponse = followerClient!!.lowLevelClient.performRequest(persistentConnectionRequest)
        assertEquals(HttpStatus.SC_OK.toLong(), persistentConnectionResponse.statusLine.statusCode.toLong())
    }
}
