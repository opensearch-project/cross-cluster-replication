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

import com.amazon.elasticsearch.replication.metadata.ReplicationMetadataManager
import com.amazon.elasticsearch.replication.metadata.store.ReplicationMetadata
import com.amazon.elasticsearch.replication.metadata.store.ReplicationStoreMetadataType
import com.amazon.opendistroforelasticsearch.commons.authuser.User
import org.apache.logging.log4j.LogManager
import org.elasticsearch.common.util.concurrent.ThreadContext

class SecurityContext(val metadataManager: ReplicationMetadataManager,
                      val metadataType: ReplicationStoreMetadataType,
                      val connection: String,
                      val resource: String) {
    companion object {
        const val INJECTED_USER = "injected_user"
        const val OPENDISTRO_USER_INFO = "_opendistro_security_user_info"
        const val OPENDISTRO_USER_INFO_DELIMITOR = "|"

        private val log = LogManager.getLogger(SecurityContext::class.java)

        fun fromSecurityThreadContext(threadContext: ThreadContext): User? {
            val userInfo = threadContext.getTransient<String?>(OPENDISTRO_USER_INFO)
            return User.parse(userInfo)
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

    suspend fun fromReplicationMetadata(): String? {
        var metadata: ReplicationMetadata? = if(metadataType == ReplicationStoreMetadataType.AUTO_FOLLOW) {
            metadataManager.getAutofollowMetadata(resource, connection)
        } else {
            metadataManager.getIndexReplicationMetadata(resource, connection)
        }
        val user = metadata?.followerContext?.user
        return user?.toInjectedUser()
    }

    fun fromReplicationMetadata(timeout: Long): String? {
        val metadata = metadataManager.getIndexReplicationMetadata(resource, connection, timeout)
        val user = metadata.followerContext.user
        if(user != null) {
            return user.toInjectedUser()
        }
        return null
    }
}
