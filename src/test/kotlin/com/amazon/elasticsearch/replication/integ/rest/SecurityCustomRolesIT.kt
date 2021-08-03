package com.amazon.elasticsearch.replication.integ.rest

import com.amazon.elasticsearch.replication.*
import org.apache.http.HttpStatus
import org.apache.http.entity.ContentType
import org.apache.http.message.BasicHeader
import org.apache.http.nio.entity.NStringEntity
import org.assertj.core.api.Assertions
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest
import org.elasticsearch.client.Request
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.ResponseException
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.test.ESTestCase
import org.junit.Assert
import org.junit.Assume
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.concurrent.TimeUnit

@MultiClusterAnnotations.ClusterConfigurations(
        MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
        MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)
class SecurityCustomRolesIT: SecurityBase()  {
    private val leaderIndexName = "leader_index"

    fun `test START replication works for user with valid permissions`() {
        Assume.assumeTrue(isSecurityEnabled)
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        val followerIndexName = "follower-index1"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                    assumeRoles = AssumeRoles(remoteClusterRole = "role1",localClusterRole = "role1"))
            var requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
            var basicAuthHeader = BasicHeader("Authorization",
                    "Basic " + Base64.getEncoder().encodeToString("testUser1:password".toByteArray(StandardCharsets.UTF_8)))
            requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)

            followerClient.startReplication(startReplicationRequest, requestOptions= requestOptionsBuilder.build())
            ESTestCase.assertBusy {
                Assertions.assertThat(followerClient.indices().exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT)).isEqualTo(true)
            }
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test START replication is forbidden for user with invalid permissions`() {
        Assume.assumeTrue(isSecurityEnabled)
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()

        var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                assumeRoles = AssumeRoles(remoteClusterRole = "role1",localClusterRole = "role1"))
        var requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
        var basicAuthHeader = BasicHeader("Authorization",
                    "Basic " + Base64.getEncoder().encodeToString("testUser2:password".toByteArray(StandardCharsets.UTF_8)))
        requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)

        Assertions.assertThatThrownBy { followerClient.startReplication(startReplicationRequest, requestOptions= requestOptionsBuilder.build()) }
                    .isInstanceOf(ResponseException::class.java)
                    .hasMessageContaining("403 Forbidden")
    }

    fun `test STOP replication works for user with valid permissions`() {
        Assume.assumeTrue(isSecurityEnabled)
        val followerClient = getClientForCluster(FOLLOWER)
        var requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
        var basicAuthHeader = BasicHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString("testUser1:password".toByteArray(StandardCharsets.UTF_8)))
        requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)
        Assertions.assertThatThrownBy {
            followerClient.stopReplication("no_index")
        }.isInstanceOf(ResponseException::class.java)
                .hasMessageContaining("No replication in progress for index:no_index")
    }

    fun `test STOP replication is forbidden for user with invalid permissions`() {
        Assume.assumeTrue(isSecurityEnabled)
        val followerClient = getClientForCluster(FOLLOWER)
        var requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
        var basicAuthHeader = BasicHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString("testUser2:password".toByteArray(StandardCharsets.UTF_8)))
        requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)
        Assertions.assertThatThrownBy {
            followerClient.stopReplication("follower-index1", requestOptions = requestOptionsBuilder.build())
        }.isInstanceOf(ResponseException::class.java)
        .hasMessageContaining("403 Forbidden")
    }

    fun `test PAUSE replication works for user with valid permissions`() {
        Assume.assumeTrue(isSecurityEnabled)
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                    assumeRoles = AssumeRoles(remoteClusterRole = "role1",localClusterRole = "role1"))
            var requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
            var basicAuthHeader = BasicHeader("Authorization",
                    "Basic " + Base64.getEncoder().encodeToString("testUser1:password".toByteArray(StandardCharsets.UTF_8)))
            requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)

            followerClient.startReplication(startReplicationRequest, requestOptions= requestOptionsBuilder.build(), waitForRestore = true)

            /* At this point, the follower cluster should be in FOLLOWING state. Next, we pause replication
            and verify the same
             */
            followerClient.pauseReplication(followerIndexName, requestOptions = requestOptionsBuilder.build())

            // Validate paused replication using Status Api
            assertBusy {
                `validate aggregated paused status response`(followerClient.replicationStatus(followerIndexName,requestOptions = requestOptionsBuilder.build()))
            }
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test PAUSE replication is forbidden for user with invalid permissions`() {
        Assume.assumeTrue(isSecurityEnabled)
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                    assumeRoles = AssumeRoles(remoteClusterRole = "role1",localClusterRole = "role1"))
            var requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
            var basicAuthHeader = BasicHeader("Authorization",
                    "Basic " + Base64.getEncoder().encodeToString("testUser1:password".toByteArray(StandardCharsets.UTF_8)))
            requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)

            followerClient.startReplication(startReplicationRequest, requestOptions= requestOptionsBuilder.build(), waitForRestore = true)

            requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
            basicAuthHeader = BasicHeader("Authorization",
                    "Basic " + Base64.getEncoder().encodeToString("testUser2:password".toByteArray(StandardCharsets.UTF_8)))
            requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)

            Assertions.assertThatThrownBy {
                followerClient.pauseReplication(followerIndexName, requestOptions = requestOptionsBuilder.build())
            }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining("403 Forbidden")

        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test STATUS Api works for user with valid permissions`() {
        Assume.assumeTrue(isSecurityEnabled)
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                    assumeRoles = AssumeRoles(remoteClusterRole = "role1",localClusterRole = "role1"))
            var requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
            var basicAuthHeader = BasicHeader("Authorization",
                    "Basic " + Base64.getEncoder().encodeToString("testUser1:password".toByteArray(StandardCharsets.UTF_8)))
            requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)

            followerClient.startReplication(startReplicationRequest, requestOptions= requestOptionsBuilder.build(), waitForRestore = true)

            assertBusy {
                `validate status syncing response`(followerClient.replicationStatus(followerIndexName,requestOptions = requestOptionsBuilder.build()))
            }
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test STATUS Api is forbidden for user with invalid permissions`() {
        Assume.assumeTrue(isSecurityEnabled)
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                    assumeRoles = AssumeRoles(remoteClusterRole = "role1",localClusterRole = "role1"))
            var requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
            var basicAuthHeader = BasicHeader("Authorization",
                    "Basic " + Base64.getEncoder().encodeToString("testUser1:password".toByteArray(StandardCharsets.UTF_8)))
            requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)

            followerClient.startReplication(startReplicationRequest, requestOptions= requestOptionsBuilder.build(), waitForRestore = true)

            requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
            basicAuthHeader = BasicHeader("Authorization",
                    "Basic " + Base64.getEncoder().encodeToString("testUser2:password".toByteArray(StandardCharsets.UTF_8)))
            requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)
            Assertions.assertThatThrownBy {
                followerClient.replicationStatus(followerIndexName,requestOptions = requestOptionsBuilder.build())
            }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining("403 Forbidden")
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test UPDATE settings works for user with valid permissions`() {
        Assume.assumeTrue(isSecurityEnabled)
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
            var requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
            var basicAuthHeader = BasicHeader("Authorization",
                    "Basic " + Base64.getEncoder().encodeToString("testUser1:password".toByteArray(StandardCharsets.UTF_8)))
            requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName,
                assumeRoles = AssumeRoles(remoteClusterRole = "role1",localClusterRole = "role1")),
                requestOptions = requestOptionsBuilder.build())
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
            followerClient.updateReplication(followerIndexName, settings, requestOptionsBuilder.build())

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

    fun `test UPDATE settings is forbidden for user with invalid permissions`() {
        Assume.assumeTrue(isSecurityEnabled)
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
            var requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
            var basicAuthHeader = BasicHeader("Authorization",
                    "Basic " + Base64.getEncoder().encodeToString("testUser1:password".toByteArray(StandardCharsets.UTF_8)))
            requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName,
                    assumeRoles = AssumeRoles(remoteClusterRole = "role1",localClusterRole = "role1")),
                    requestOptions = requestOptionsBuilder.build())
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

            requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
            basicAuthHeader = BasicHeader("Authorization",
                    "Basic " + Base64.getEncoder().encodeToString("testUser2:password".toByteArray(StandardCharsets.UTF_8)))
            requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)
            Assertions.assertThatThrownBy {
                followerClient.updateReplication(followerIndexName, settings, requestOptionsBuilder.build())
            }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining("403 Forbidden")
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test REVOKE replay permission`() {
        Assume.assumeTrue(isSecurityEnabled)
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                    assumeRoles = AssumeRoles(remoteClusterRole = "role1",localClusterRole = "role1"))
            var requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
            var basicAuthHeader = BasicHeader("Authorization",
                    "Basic " + Base64.getEncoder().encodeToString("testUser1:password".toByteArray(StandardCharsets.UTF_8)))
            requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)

            followerClient.startReplication(startReplicationRequest, requestOptions= requestOptionsBuilder.build(), waitForRestore = true)

            insertDocToIndex(LEADER, "1", "dummy data 1",leaderIndexName)
            //Querying ES cluster throws random exceptions like MasterNotDiscovered or ShardsFailed etc, so catching them and retrying
            assertBusy ({
                try {
                    Assertions.assertThat(docs(FOLLOWER, followerIndexName)).contains("dummy data 1")
                } catch (ex: Exception) {
                    Assertions.assertThat(true).isEqualTo(false)
                }
            }, 1, TimeUnit.MINUTES)

            assertBusy {
                `validate status syncing response`(followerClient.replicationStatus(followerIndexName,requestOptions = requestOptionsBuilder.build()))
            }

            updateRole(followerIndexName,"role1", false)
            insertDocToIndex(LEADER, "2", "dummy data 2",leaderIndexName)

            assertBusy ({
                validatePausedState(followerClient.replicationStatus(followerIndexName,requestOptions = requestOptionsBuilder.build()))
            }, 100, TimeUnit.SECONDS)

            updateRole(followerIndexName,"role1", true)
        } finally {
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
