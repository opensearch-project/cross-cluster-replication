package com.amazon.elasticsearch.replication.integ.rest

import com.amazon.elasticsearch.replication.MultiClusterRestTestCase
import org.apache.http.HttpStatus
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.elasticsearch.client.Request
import org.junit.BeforeClass

abstract class SecurityBase : MultiClusterRestTestCase()   {
    companion object{
        fun addSecurityRoles() {
            addUserToRole("testUser1","role1", FOLLOWER)
            addUserToRole("testUser2","role2", FOLLOWER)
            addUserToRole("testUser1","role1", LEADER)
        }
        @BeforeClass @JvmStatic
        fun setupSecurity() {
            if(isSecurityEnabled) {
                addUsers()
                createRoles()
                addSecurityRoles()
            }
        }
        fun createRoles() {
            createRoleWithPermissions("follower-index1", "role1")
            createLeaderRoleWithPermissions("*", "role1")
            createRoleWithPermissions("follower-index2", "role2")
        }

        private fun createLeaderRoleWithPermissions(indexPattern: String, role: String) {
            val leaderClient = testClusters.get(LEADER)
            val persistentConnectionRequest = Request("PUT", "_opendistro/_security/api/roles/"+role)
            val entityAsString = """
            {
                "index_permissions": [
                    {
                        "index_patterns": ["$indexPattern"],
                        "allowed_actions": [
                            "indices:admin/plugins/replication/index/setup/validate",
                            "indices:data/read/plugins/replication/changes",
                            "indices:data/read/plugins/replication/file_chunk"
                        ]
                    }
                ]
            }
            """.trimMargin()
            persistentConnectionRequest.entity = NStringEntity(entityAsString, ContentType.APPLICATION_JSON)
            val persistentConnectionResponse = leaderClient!!.lowLevelClient.performRequest(persistentConnectionRequest)
            assertEquals(HttpStatus.SC_CREATED.toLong(), persistentConnectionResponse.statusLine.statusCode.toLong())
        }

        private fun createRoleWithPermissions(indexPattern: String, role: String) {
            val followerClient = testClusters.get(FOLLOWER)
            val persistentConnectionRequest = Request("PUT", "_opendistro/_security/api/roles/"+role)

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
                            "indices:data/write/plugins/replication/changes",
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
            assertEquals(HttpStatus.SC_CREATED.toLong(), persistentConnectionResponse.statusLine.statusCode.toLong())
        }

        private fun addUserToRole(user: String, role: String, clusterName: String) {
            val followerClient = testClusters.get(clusterName)
            val persistentConnectionRequest = Request("PUT", "_opendistro/_security/api/rolesmapping/"+role)
            val entityAsString = """
                {"users": ["$user"]}
            """.trimMargin()

            persistentConnectionRequest.entity = NStringEntity(entityAsString, ContentType.APPLICATION_JSON)
            val persistentConnectionResponse = followerClient!!.lowLevelClient.performRequest(persistentConnectionRequest)
            assertEquals(HttpStatus.SC_CREATED.toLong(), persistentConnectionResponse.statusLine.statusCode.toLong())
        }

        private fun addUsers(){
            addUserToCluster("testUser1","password", FOLLOWER)
            addUserToCluster("testUser1","password", LEADER)
            addUserToCluster("testUser2","password", FOLLOWER)
            addUserToCluster("testUser2","password", LEADER)
        }

        private fun addUserToCluster(userName: String, password: String, clusterName: String) {
            val followerClient = testClusters.get(clusterName)
            val persistentConnectionRequest = Request("PUT", "_opendistro/_security/api/internalusers/"+userName)
            val entityAsString = """
            {
                "password":"$password"
            }
            """.trimMargin()

            persistentConnectionRequest.entity = NStringEntity(entityAsString, ContentType.APPLICATION_JSON)
            val persistentConnectionResponse = followerClient!!.lowLevelClient.performRequest(persistentConnectionRequest)
            assertEquals(HttpStatus.SC_CREATED.toLong(), persistentConnectionResponse.statusLine.statusCode.toLong())
        }
    }
}