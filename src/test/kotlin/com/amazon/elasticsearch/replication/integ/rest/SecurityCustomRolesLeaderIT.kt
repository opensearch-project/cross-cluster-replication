package com.amazon.elasticsearch.replication.integ.rest

import com.amazon.elasticsearch.replication.*
import com.amazon.elasticsearch.replication.util.addBasicAuthHeader
import org.apache.http.HttpStatus
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.assertj.core.api.Assertions
import org.elasticsearch.client.Request
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.ResponseException
import org.elasticsearch.client.indices.CreateIndexRequest
import org.junit.Assert
import org.junit.Assume
import org.junit.Before
import java.util.concurrent.TimeUnit

@MultiClusterAnnotations.ClusterConfigurations(
        MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
        MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)
class SecurityCustomRolesLeaderIT: SecurityBase() {
    private val leaderIndexName = "leader_index"
    @Before
    fun beforeTest() {
        Assume.assumeTrue(isSecurityPropertyEnabled)
    }

    fun `test for LEADER that START replication is forbidden for user with invalid permissions`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()

        var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                assumeRoles = AssumeRoles(leaderClusterRole = "leaderRoleNoPerms",followerClusterRole = "followerRoleValidPerms"))

        Assertions.assertThatThrownBy { followerClient.startReplication(startReplicationRequest,
                requestOptions= RequestOptions.DEFAULT.addBasicAuthHeader("testUser6","password")) }
                .isInstanceOf(ResponseException::class.java)
                .hasMessageContaining("403 Forbidden")
                .hasMessageContaining("no permissions for [indices:admin/plugins/replication/index/setup/validate]")
    }

    fun `test for LEADER that REVOKE replay permission`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                    assumeRoles = AssumeRoles(leaderClusterRole = "leaderRoleValidPerms",followerClusterRole = "followerRoleValidPerms"))

            followerClient.startReplication(startReplicationRequest, waitForRestore = true,
                    requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1","password"))

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
                `validate status syncing response`(followerClient.replicationStatus(followerIndexName,
                        requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1","password")))
            }

            updateRole(followerIndexName,"leaderRoleValidPerms", false)
            insertDocToIndex(LEADER, "2", "dummy data 2",leaderIndexName)

            assertBusy ({
                validatePausedState(followerClient.replicationStatus(followerIndexName,
                        requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1","password")))
            }, 100, TimeUnit.SECONDS)

            updateRole(followerIndexName,"leaderRoleValidPerms", true)
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test for LEADER REVOKE bookstrap permission`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                    assumeRoles = AssumeRoles(leaderClusterRole = "leaderRoleValidPerms",followerClusterRole = "followerRoleValidPerms"))

            updateFileChunkPermissions("","leaderRoleValidPerms", false)
            followerClient.startReplication(startReplicationRequest,
                    requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1","password"))

            assertBusy ({
                validateNotInProgressState(followerClient.replicationStatus(followerIndexName,
                        requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1","password")))
            }, 10, TimeUnit.SECONDS)

            updateFileChunkPermissions("","leaderRoleValidPerms", true)
        } catch (ex : Exception) {
            logger.info("Exception is", ex)
        } finally {
                followerClient.stopReplication(followerIndexName)
        }
    }

    private fun updateFileChunkPermissions(indexPattern: String, role: String, shouldAddfilechunkPermission: Boolean) {
        val followerClient = testClusters.get(LEADER)
        val persistentConnectionRequest = Request("PUT", "_opendistro/_security/api/roles/"+role)
        var fileChunkPermission : String = ""
        if(shouldAddfilechunkPermission)
            fileChunkPermission = "\"indices:data/read/plugins/replication/file_chunk\","
        val entityAsString = """
            {
                "index_permissions": [
                    {
                        "index_patterns": ["$indexPattern*"],
                        "allowed_actions": [
                            "indices:admin/plugins/replication/index/setup/validate",
                            $fileChunkPermission
                            "indices:data/read/plugins/replication/changes"
                        ]
                    }
                ]
            }
            """.trimMargin()
        persistentConnectionRequest.entity = NStringEntity(entityAsString, ContentType.APPLICATION_JSON)
        val persistentConnectionResponse = followerClient!!.lowLevelClient.performRequest(persistentConnectionRequest)
        assertEquals(HttpStatus.SC_OK.toLong(), persistentConnectionResponse.statusLine.statusCode.toLong())
    }

    private fun updateRole(indexPattern: String, role: String, shouldAddReadPermission: Boolean) {
        val followerClient = testClusters.get(LEADER)
        val persistentConnectionRequest = Request("PUT", "_opendistro/_security/api/roles/"+role)
        var readPermission : String = ""
        if(shouldAddReadPermission)
            readPermission = "\"indices:data/read/plugins/replication/changes\","
        val entityAsString = """
            {
                "index_permissions": [
                    {
                        "index_patterns": ["$indexPattern*"],
                        "allowed_actions": [
                            "indices:admin/plugins/replication/index/setup/validate",
                            $readPermission
                            "indices:data/read/plugins/replication/file_chunk"
                        ]
                    }
                ]
            }
            """.trimMargin()
        persistentConnectionRequest.entity = NStringEntity(entityAsString, ContentType.APPLICATION_JSON)
        val persistentConnectionResponse = followerClient!!.lowLevelClient.performRequest(persistentConnectionRequest)
        assertEquals(HttpStatus.SC_OK.toLong(), persistentConnectionResponse.statusLine.statusCode.toLong())
    }

    private fun validatePausedState(statusResp: Map<String, Any>) {
        Assert.assertEquals(statusResp.getValue("status"),"PAUSED")
        Assert.assertTrue((statusResp.getValue("reason")).toString().contains("no permissions for [indices:data/read/plugins/replication/changes] and associated roles"))
        Assert.assertFalse(statusResp.containsKey("shard_replication_details"))
        Assert.assertFalse(statusResp.containsKey("local_checkpoint"))
        Assert.assertFalse(statusResp.containsKey("remote_checkpoint"))
    }

    private fun validateNotInProgressState(statusResp: Map<String, Any>) {
        Assert.assertEquals(statusResp.getValue("status"),"REPLICATION NOT IN PROGRESS")
        Assert.assertTrue((statusResp.getValue("reason")).toString().contains("no permissions for [indices:data/read/plugins/replication/changes] and associated roles"))
        Assert.assertFalse(statusResp.containsKey("shard_replication_details"))
        Assert.assertFalse(statusResp.containsKey("local_checkpoint"))
        Assert.assertFalse(statusResp.containsKey("remote_checkpoint"))
    }
}