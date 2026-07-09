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
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.ResponseException
import org.opensearch.client.indices.CreateIndexRequest
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
                useRoles = UseRoles(leaderClusterRole = "leaderRoleNoPerms",followerClusterRole = "followerRoleValidPerms"))
        Assertions.assertThatThrownBy { followerClient.startReplication(startReplicationRequest,
                requestOptions= RequestOptions.DEFAULT.addBasicAuthHeader("testUser6",INTEG_TEST_PASSWORD)) }
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
            updateRole(followerIndexName,"leaderRoleValidPerms", false)
            insertDocToIndex(LEADER, "2", "dummy data 2",leaderIndexName)
            assertBusy ({
                validatePausedState(followerClient.replicationStatus(followerIndexName,
                        requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1",INTEG_TEST_PASSWORD)))
            }, 100, TimeUnit.SECONDS)
        } finally {
            updateRole(followerIndexName,"leaderRoleValidPerms", true)
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
                    useRoles = UseRoles(leaderClusterRole = "leaderRoleValidPerms",followerClusterRole = "followerRoleValidPerms"))
            updateFileChunkPermissions("","leaderRoleValidPerms", false)
            followerClient.startReplication(startReplicationRequest,
                    requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1",INTEG_TEST_PASSWORD))
            assertBusy ({
                validateFailedState(followerClient.replicationStatus(followerIndexName,
                        requestOptions = RequestOptions.DEFAULT.addBasicAuthHeader("testUser1",INTEG_TEST_PASSWORD)))
            }, 60, TimeUnit.SECONDS)
        } catch (ex : Exception) {
            logger.info("Exception is", ex)
            Assert.assertNull(ex)
        } finally {
            updateFileChunkPermissions("","leaderRoleValidPerms", true)
        }
    }

    private fun updateFileChunkPermissions(indexPattern: String, role: String, shouldAddfilechunkPermission: Boolean) {
        val followerClient = testClusters.get(LEADER)
        val persistentConnectionRequest = Request("PUT", "_plugins/_security/api/roles/"+role)
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
        persistentConnectionRequest.entity = StringEntity(entityAsString, ContentType.APPLICATION_JSON)
        val persistentConnectionResponse = followerClient!!.lowLevelClient.performRequest(persistentConnectionRequest)
        assertEquals(HttpStatus.SC_OK.toLong(), persistentConnectionResponse.statusLine.statusCode.toLong())
    }

    private fun updateRole(indexPattern: String, role: String, shouldAddReadPermission: Boolean) {
        val followerClient = testClusters.get(LEADER)
        val persistentConnectionRequest = Request("PUT", "_plugins/_security/api/roles/"+role)
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
        persistentConnectionRequest.entity = StringEntity(entityAsString, ContentType.APPLICATION_JSON)
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
        Assert.assertFalse(statusResp.containsKey("reason"))
        Assert.assertFalse(statusResp.containsKey("shard_replication_details"))
        Assert.assertFalse(statusResp.containsKey("local_checkpoint"))
        Assert.assertFalse(statusResp.containsKey("remote_checkpoint"))
    }

    private fun validateFailedState(statusResp: Map<String, Any>) {
        Assert.assertEquals("FAILED", statusResp.getValue("status"))
    }
}
