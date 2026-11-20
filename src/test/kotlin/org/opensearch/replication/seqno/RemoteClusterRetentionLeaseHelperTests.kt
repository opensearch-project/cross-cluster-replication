/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.replication.seqno

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import kotlinx.coroutines.runBlocking
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionType
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.core.index.shard.ShardId
import org.opensearch.index.seqno.RetentionLeaseActions
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.client.NoOpNodeClient

/** Checks that the retention lease calls on the leader run in a default context, not in the caller's. */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class RemoteClusterRetentionLeaseHelperTests : OpenSearchTestCase() {

    private val identities = mutableListOf<String>()

    fun testRenewAndRemoveRunWithoutTheCallersContext() {
        val client = object : NoOpNodeClient("retention-lease") {
            @Suppress("UNCHECKED_CAST")
            override fun <Request : ActionRequest, Response : ActionResponse> doExecute(
                action: ActionType<Response>,
                request: Request,
                listener: ActionListener<Response>
            ) {
                identities.add("${action.name()} as ${threadPool().threadContext.getTransient<String>("identity")}")
                listener.onResponse(RetentionLeaseActions.Response() as Response)
            }
        }
        try {
            client.threadPool().threadContext.putTransient("identity", "caller")
            val helper = RemoteClusterRetentionLeaseHelper("follower", "uuid", client)
            val leaderShard = ShardId("leader", "_na_", 0)
            val followerShard = ShardId("follower", "_na_", 0)

            runBlocking {
                helper.renewRetentionLease(leaderShard, 1, followerShard)
                helper.attemptRetentionLeaseRemoval(leaderShard, followerShard)
            }

            assertEquals(
                listOf("${RetentionLeaseActions.Renew.ACTION_NAME} as null", "${RetentionLeaseActions.Remove.ACTION_NAME} as null"),
                identities
            )
            assertEquals("caller", client.threadPool().threadContext.getTransient<String>("identity"))
        } finally {
            client.close()
        }
    }
}
