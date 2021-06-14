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

package com.amazon.elasticsearch.replication.util

import com.amazon.elasticsearch.replication.action.changes.GetChangesAction
import com.amazon.elasticsearch.replication.action.index.ReplicateIndexAction
import com.amazon.elasticsearch.replication.action.replay.ReplayChangesAction
import com.amazon.elasticsearch.replication.action.repository.GetFileChunkAction
import com.amazon.elasticsearch.replication.action.repository.GetStoreMetadataAction
import com.amazon.elasticsearch.replication.metadata.ReplicationMetadataManager
import com.amazon.elasticsearch.replication.metadata.store.ReplicationMetadata
import com.amazon.elasticsearch.replication.metadata.store.ReplicationStoreMetadataType
import com.amazon.opendistroforelasticsearch.commons.ConfigConstants
import com.amazon.opendistroforelasticsearch.commons.authuser.User
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.action.ActionType
import org.elasticsearch.common.util.concurrent.ThreadContext
import org.elasticsearch.transport.RemoteClusterAwareRequest

class SecurityContext {
    companion object {
        private val log = LogManager.getLogger(SecurityContext::class.java)
        const val OPENDISTRO_SECURITY_USER = "_opendistro_security_user"
        const val OPENDISTRO_SECURITY_ASSUME_ROLES = "opendistro_security_assume_roles"

        val ADMIN_USER = User("ccr_user", null, listOf("all_access"), null)

        val ALL_TRANSIENTS = listOf(ConfigConstants.OPENDISTRO_SECURITY_INJECTED_ROLES,
                ConfigConstants.INJECTED_USER, OPENDISTRO_SECURITY_USER)

        val LEADER_USER_ACTIONS = listOf(GetChangesAction.NAME, GetFileChunkAction.NAME)
        val FOLLOWER_USER_ACTIONS = listOf(ReplayChangesAction.NAME, ReplicateIndexAction.NAME)

        fun fromSecurityThreadContext(threadContext: ThreadContext): User? {
            val userInfo = threadContext.getTransient<String?>(ConfigConstants.OPENDISTRO_SECURITY_USER_INFO_THREAD_CONTEXT)
            return User.parse(userInfo)
        }

        fun asUserInjection(threadContext: ThreadContext, userString: String?) {
            if(userString != null) {
                val userInfo = threadContext.getTransient<String?>(ConfigConstants.INJECTED_USER)
                if (userInfo != null) {
                    log.warn("Injected user not empty in thread context $userInfo")
                }
                else {
                    threadContext.putTransient(ConfigConstants.INJECTED_USER, userString)
                }
            }
        }

        fun asRolesInjection(threadContext: ThreadContext, role: String?) {
            if(role != null) {
                val rolesInj = threadContext.getTransient<String?>(ConfigConstants.OPENDISTRO_SECURITY_INJECTED_ROLES)
                if(rolesInj != null) {
                    log.warn("Injected roles not empty in thread context $rolesInj")
                }
                else {
                    threadContext.putTransient(ConfigConstants.OPENDISTRO_SECURITY_INJECTED_ROLES, role)
                }
            }
        }

        fun setBasedOnActions(replMetadata: ReplicationMetadata?, action: String, threadContext: ThreadContext) {
            if(replMetadata != null) {
                if(LEADER_USER_ACTIONS.contains(action)) {
                    asRolesInjection(threadContext, replMetadata.leaderContext.user?.toInjectedRoles())
                    return
                } else if(FOLLOWER_USER_ACTIONS.contains(action)) {
                    asRolesInjection(threadContext, replMetadata.followerContext.user?.toInjectedRoles())
                    return
                }
            }
            // For all other requests - using admin
            asRolesInjection(threadContext, ADMIN_USER.toInjectedRoles())
        }
    }
}
