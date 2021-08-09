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

package org.opensearch.replication.util

import org.opensearch.replication.action.autofollow.UpdateAutoFollowPatternAction
import org.opensearch.replication.action.changes.GetChangesAction
import org.opensearch.replication.action.index.ReplicateIndexAction
import org.opensearch.replication.action.pause.PauseIndexReplicationAction
import org.opensearch.replication.action.replay.ReplayChangesAction
import org.opensearch.replication.action.repository.GetFileChunkAction
import org.opensearch.replication.action.repository.GetStoreMetadataAction
import org.opensearch.replication.action.resume.ResumeIndexReplicationAction
import org.opensearch.replication.action.status.ReplicationStatusAction
import org.opensearch.replication.action.stop.StopIndexReplicationAction
import org.opensearch.replication.action.update.UpdateIndexReplicationAction
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.metadata.store.ReplicationMetadata
import org.opensearch.replication.metadata.store.ReplicationStoreMetadataType
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionResponse
import org.opensearch.action.ActionType
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.transport.RemoteClusterAwareRequest

class SecurityContext {
    companion object {
        private val log = LogManager.getLogger(SecurityContext::class.java)
        const val OPENDISTRO_SECURITY_USER = "_opendistro_security_user"
        const val OPENDISTRO_SECURITY_ASSUME_ROLES = "opendistro_security_assume_roles"
        const val REPLICATION_PLUGIN_USER = "ccr_user"

        val ADMIN_USER = User(REPLICATION_PLUGIN_USER, null, listOf("all_access"), null)

        val ALL_TRANSIENTS = listOf(ConfigConstants.OPENSEARCH_SECURITY_INJECTED_ROLES,
                ConfigConstants.INJECTED_USER, OPENDISTRO_SECURITY_USER)

        val LEADER_USER_ACTIONS = listOf(GetChangesAction.NAME, GetFileChunkAction.NAME)
        val FOLLOWER_USER_ACTIONS = listOf(ReplayChangesAction.NAME,
                ReplicateIndexAction.NAME, PauseIndexReplicationAction.NAME,
                ResumeIndexReplicationAction.NAME, StopIndexReplicationAction.NAME,
                UpdateIndexReplicationAction.NAME, ReplicationStatusAction.NAME,
                UpdateAutoFollowPatternAction.NAME)

        fun fromSecurityThreadContext(threadContext: ThreadContext): User? {
            val userInfo = threadContext.getTransient<String?>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
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
                val rolesInj = threadContext.getTransient<String?>(ConfigConstants.OPENSEARCH_SECURITY_INJECTED_ROLES)
                if(rolesInj != null) {
                    log.warn("Injected roles not empty in thread context $rolesInj")
                }
                else {
                    threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_INJECTED_ROLES, role)
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
