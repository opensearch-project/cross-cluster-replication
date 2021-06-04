/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.replication.util

import org.opensearch.replication.metadata.ReplicationMetadata
import org.apache.logging.log4j.LogManager
import org.opensearch.cluster.ClusterState
import org.opensearch.common.util.concurrent.ThreadContext

interface SecurityContext {
    companion object {
        const val INJECTED_USER = "injected_user"
        const val OPENDISTRO_USER_INFO = "_opendistro_security_user_info"
        const val OPENDISTRO_USER_INFO_DELIMITOR = "|"

        private val log = LogManager.getLogger(SecurityContext::class.java)

        fun fromSecurityThreadContext(threadContext: ThreadContext): String? {
            // Directly return injected_user from the thread context if the user info is not set.
            val userInfo = threadContext.getTransient<String?>(OPENDISTRO_USER_INFO) ?: return threadContext.getTransient(INJECTED_USER)
            val usersAndRoles = userInfo.split(OPENDISTRO_USER_INFO_DELIMITOR)
            var userName: String
            var userBackendRoles = ""
            if(usersAndRoles.isEmpty()) {
                log.warn("Failed to parse security user info - $userInfo")
                return null
            }
            userName = usersAndRoles[0]
            if(usersAndRoles.size >= 2) {
                userBackendRoles = usersAndRoles[1]
            }
            return "${userName}${OPENDISTRO_USER_INFO_DELIMITOR}${userBackendRoles}"
        }

        fun fromClusterState(clusterState: ClusterState, remoteCluster: String, followerIndex: String): String? {
            val replicationMetadata = clusterState.metadata.custom<ReplicationMetadata>(ReplicationMetadata.NAME)
            return replicationMetadata?.securityContexts?.get(remoteCluster)?.get(followerIndex)
        }

        fun toThreadContext(threadContext: ThreadContext, injectedUser: String?) {
            if(injectedUser != null) {
                val userInfo = threadContext.getTransient<String?>(INJECTED_USER)
                if (userInfo != null) {
                    log.warn("Injected user not empty in thread context $userInfo")
                }
                else {
                    threadContext.putTransient(INJECTED_USER, injectedUser)
                }
            }
        }
    }
}
