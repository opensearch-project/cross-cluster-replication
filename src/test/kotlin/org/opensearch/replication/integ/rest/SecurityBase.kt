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

import org.opensearch.replication.MultiClusterRestTestCase
import org.apache.hc.core5.http.HttpStatus
import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.opensearch.client.Request
import org.junit.BeforeClass
import org.opensearch.commons.replication.action.ReplicationActions.STOP_REPLICATION_ACTION_NAME

const val INTEG_TEST_PASSWORD = "ccr-integ-test@123"

abstract class SecurityBase : MultiClusterRestTestCase()   {
    companion object {
        var initialized : Boolean = false

        fun addSecurityRoles() {
            addUserToRole("testUser2","followerRoleNoPerms", FOLLOWER)
            addUserToRole("testUser1\",\"testUser3\",\"testUser4\",\"testUser5\",\"testUser7","leaderRoleValidPerms", LEADER)
            addUserToRole("testUser3","followerDlsRole", FOLLOWER)
            addUserToRole("testUser4","followerFlsRole", FOLLOWER)
            addUserToRole("testUser5","followerFieldMaskRole", FOLLOWER)
            addUserToRole("testUser7","followerFieldMaskRole2", FOLLOWER)
            addUserToRole("testUser6","leaderRoleNoPerms", LEADER)
            addUserToRole("testUser1\",\"testUser6","followerRoleValidPerms", FOLLOWER)
        }

        @BeforeClass @JvmStatic
        fun setupSecurity() {
            if(isSecurityPropertyEnabled && (!initialized || forceInitSecurityConfiguration)) {
                addUsers()
                createRoles()
                addSecurityRoles()
            }
            initialized = true;
        }

        fun createRoles() {
            createRoleWithPermissions("follower-index1", "followerRoleValidPerms")
            createLeaderRoleWithPermissions("", "leaderRoleValidPerms")
            createLeaderRoleWithNoPermissions("", "leaderRoleNoPerms")
            createRoleWithPermissions("follower-index2", "followerRoleNoPerms")
            createDLSRole("follower-index1", "followerDlsRole")
            createFLSRole("follower-index1", "followerFlsRole")
            createFieldMaskingRole("follower-index1", "followerFieldMaskRole")
            createFieldMaskingRoleForIndex("follower-index2", "follower-index1","followerFieldMaskRole2")
        }

        private fun createFieldMaskingRole(indexPattern: String, role: String) {
            val leaderClient = testClusters.get(FOLLOWER)
            val persistentConnectionRequest = Request("PUT", "_plugins/_security/api/roles/"+role)
            val entityAsString = """
            {
                "cluster_permissions": [
                    "cluster:admin/plugins/replication/autofollow/update"
                ],
                "index_permissions": [
                    {
                        "index_patterns": ["$indexPattern*"],
                        "fls":["Designation"],
                        "allowed_actions": [
                            "indices:admin/plugins/replication/index/setup/validate",
                            "indices:data/write/plugins/replication/changes",
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
            val persistentConnectionResponse = leaderClient!!.lowLevelClient.performRequest(persistentConnectionRequest)
            assertTrue(HttpStatus.SC_CREATED.toLong() == persistentConnectionResponse.statusLine.statusCode.toLong() ||
                    HttpStatus.SC_OK.toLong() == persistentConnectionResponse.statusLine.statusCode.toLong())
        }

        private fun createFieldMaskingRoleForIndex(indexPatternWithFieldMasking: String, indexPatternWithoutFieldMasking:String, role: String) {
            val leaderClient = testClusters.get(FOLLOWER)
            val persistentConnectionRequest = Request("PUT", "_plugins/_security/api/roles/"+role)
            val entityAsString = """
            {
                "cluster_permissions": [
                    "cluster:admin/plugins/replication/autofollow/update"
                ],
                "index_permissions": [
                    {
                        "index_patterns": ["$indexPatternWithFieldMasking*"],
                        "fls":["Designation"],
                        "allowed_actions": [
                            "indices:admin/plugins/replication/index/setup/validate",
                            "indices:data/write/plugins/replication/changes",
                            "indices:admin/plugins/replication/index/start",
                            "indices:admin/plugins/replication/index/pause",
                            "indices:admin/plugins/replication/index/resume",
                            "$STOP_REPLICATION_ACTION_NAME",
                            "indices:admin/plugins/replication/index/update",
                            "indices:admin/plugins/replication/index/status_check"
                        ]
                    },
                    {
                        "index_patterns": ["$indexPatternWithoutFieldMasking*"],
                        "allowed_actions": [
                            "indices:admin/plugins/replication/index/setup/validate",
                            "indices:data/write/plugins/replication/changes",
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
            val persistentConnectionResponse = leaderClient!!.lowLevelClient.performRequest(persistentConnectionRequest)
            assertTrue(HttpStatus.SC_CREATED.toLong() == persistentConnectionResponse.statusLine.statusCode.toLong() ||
                    HttpStatus.SC_OK.toLong() == persistentConnectionResponse.statusLine.statusCode.toLong())
        }

        private fun createFLSRole(indexPattern: String, role: String) {
            val leaderClient = testClusters.get(FOLLOWER)
            val persistentConnectionRequest = Request("PUT", "_plugins/_security/api/roles/"+role)
            val entityAsString = """
            {
                "cluster_permissions": [
                    "cluster:admin/plugins/replication/autofollow/update"
                ],
                "index_permissions": [
                    {
                        "index_patterns" : ["$indexPattern*"],
                        "masked_fields" : ["User"],
                        "allowed_actions": [
                            "indices:admin/plugins/replication/index/setup/validate",
                            "indices:data/write/plugins/replication/changes",
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
            val persistentConnectionResponse = leaderClient!!.lowLevelClient.performRequest(persistentConnectionRequest)
            assertTrue(HttpStatus.SC_CREATED.toLong() == persistentConnectionResponse.statusLine.statusCode.toLong() ||
                    HttpStatus.SC_OK.toLong() == persistentConnectionResponse.statusLine.statusCode.toLong())
        }

        private fun createDLSRole(indexPattern: String, role: String) {
            val leaderClient = testClusters.get(FOLLOWER)
            val persistentConnectionRequest = Request("PUT", "_plugins/_security/api/roles/"+role)
            val entityAsString = """
            {
                "cluster_permissions": [
                    "cluster:admin/plugins/replication/autofollow/update"
                ],
                "index_permissions": [
                    {
                        "index_patterns": ["$indexPattern*"],
                        "dls": "{\"bool\": {\"must_not\": {\"match\": {\"Designation\": \"CEO\"}}}}",
                        "allowed_actions": [
                            "indices:admin/plugins/replication/index/setup/validate",
                            "indices:data/write/plugins/replication/changes",
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
            val persistentConnectionResponse = leaderClient!!.lowLevelClient.performRequest(persistentConnectionRequest)
            assertTrue(HttpStatus.SC_CREATED.toLong() == persistentConnectionResponse.statusLine.statusCode.toLong() ||
                    HttpStatus.SC_OK.toLong() == persistentConnectionResponse.statusLine.statusCode.toLong())
        }

        private fun createLeaderRoleWithNoPermissions(indexPattern: String, role: String) {
            val leaderClient = testClusters.get(LEADER)
            val persistentConnectionRequest = Request("PUT", "_plugins/_security/api/roles/"+role)
            val entityAsString = """
            {
                "index_permissions": [
                    {
                        "index_patterns": ["$indexPattern*"],
                        "allowed_actions": [
                        ]
                    }
                ]
            }
            """.trimMargin()
            persistentConnectionRequest.entity = StringEntity(entityAsString, ContentType.APPLICATION_JSON)
            val persistentConnectionResponse = leaderClient!!.lowLevelClient.performRequest(persistentConnectionRequest)
            assertTrue(HttpStatus.SC_CREATED.toLong() == persistentConnectionResponse.statusLine.statusCode.toLong() ||
                    HttpStatus.SC_OK.toLong() == persistentConnectionResponse.statusLine.statusCode.toLong())
        }

        private fun createLeaderRoleWithPermissions(indexPattern: String, role: String) {
            val leaderClient = testClusters.get(LEADER)
            val persistentConnectionRequest = Request("PUT", "_plugins/_security/api/roles/"+role)
            val entityAsString = """
            {
                "index_permissions": [
                    {
                        "index_patterns": ["$indexPattern*"],
                        "allowed_actions": [
                            "indices:admin/plugins/replication/index/setup/validate",
                            "indices:data/read/plugins/replication/changes",
                            "indices:data/read/plugins/replication/file_chunk"
                        ]
                    }
                ]
            }
            """.trimMargin()
            persistentConnectionRequest.entity = StringEntity(entityAsString, ContentType.APPLICATION_JSON)
            val persistentConnectionResponse = leaderClient!!.lowLevelClient.performRequest(persistentConnectionRequest)
            assertTrue(HttpStatus.SC_CREATED.toLong() == persistentConnectionResponse.statusLine.statusCode.toLong() ||
                    HttpStatus.SC_OK.toLong() == persistentConnectionResponse.statusLine.statusCode.toLong())
        }

        private fun createRoleWithPermissions(indexPattern: String, role: String) {
            val followerClient = testClusters.get(FOLLOWER)
            val persistentConnectionRequest = Request("PUT", "_plugins/_security/api/roles/"+role)
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
            assertTrue(HttpStatus.SC_CREATED.toLong() == persistentConnectionResponse.statusLine.statusCode.toLong() ||
                    HttpStatus.SC_OK.toLong() == persistentConnectionResponse.statusLine.statusCode.toLong())
        }

        private fun addUserToRole(user: String, role: String, clusterName: String) {
            val followerClient = testClusters.get(clusterName)
            val persistentConnectionRequest = Request("PUT", "_plugins/_security/api/rolesmapping/"+role)
            val entityAsString = """
                {"users": ["$user"]}
            """.trimMargin()
            persistentConnectionRequest.entity = StringEntity(entityAsString, ContentType.APPLICATION_JSON)
            val persistentConnectionResponse = followerClient!!.lowLevelClient.performRequest(persistentConnectionRequest)
            assertTrue(HttpStatus.SC_CREATED.toLong() == persistentConnectionResponse.statusLine.statusCode.toLong() ||
                    HttpStatus.SC_OK.toLong() == persistentConnectionResponse.statusLine.statusCode.toLong())
        }

        private fun addUsers(){
            addUserToCluster("testUser1", INTEG_TEST_PASSWORD, FOLLOWER)
            addUserToCluster("testUser1", INTEG_TEST_PASSWORD, LEADER)
            addUserToCluster("testUser2", INTEG_TEST_PASSWORD, FOLLOWER)
            addUserToCluster("testUser2", INTEG_TEST_PASSWORD, LEADER)
            addUserToCluster("testUser3", INTEG_TEST_PASSWORD, FOLLOWER)
            addUserToCluster("testUser4", INTEG_TEST_PASSWORD, FOLLOWER)
            addUserToCluster("testUser5", INTEG_TEST_PASSWORD, FOLLOWER)
            addUserToCluster("testUser6", INTEG_TEST_PASSWORD, LEADER)
            addUserToCluster("testUser6", INTEG_TEST_PASSWORD, FOLLOWER)
            addUserToCluster("testUser7", INTEG_TEST_PASSWORD, LEADER)
            addUserToCluster("testUser7", INTEG_TEST_PASSWORD, FOLLOWER)
        }

        private fun addUserToCluster(userName: String, password: String, clusterName: String) {
            val followerClient = testClusters.get(clusterName)
            val persistentConnectionRequest = Request("PUT", "_plugins/_security/api/internalusers/"+userName)
            val entityAsString = """
            {
                "password":"$password"
            }
            """.trimMargin()
            persistentConnectionRequest.entity = StringEntity(entityAsString, ContentType.APPLICATION_JSON)
            val persistentConnectionResponse = followerClient!!.lowLevelClient.performRequest(persistentConnectionRequest)
            assertTrue(HttpStatus.SC_CREATED.toLong() == persistentConnectionResponse.statusLine.statusCode.toLong() ||
                    HttpStatus.SC_OK.toLong() == persistentConnectionResponse.statusLine.statusCode.toLong())
        }
    }
}
