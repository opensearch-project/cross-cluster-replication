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

import org.assertj.core.api.Assertions.assertThat
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.client.RequestOptions
import org.opensearch.client.RestHighLevelClient
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.client.indices.GetIndexRequest
import org.opensearch.cluster.health.ClusterHealthStatus
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.index.mapper.MapperService
import org.opensearch.replication.IndexUtil
import org.opensearch.replication.MultiClusterAnnotations
import org.opensearch.replication.MultiClusterRestTestCase
import org.opensearch.replication.StartReplicationRequest
import org.opensearch.replication.getIndexReplicationTask
import org.opensearch.replication.startReplication
import org.opensearch.replication.stopReplication
import org.opensearch.test.OpenSearchTestCase.assertBusy
import java.util.concurrent.TimeUnit

/**
 * Covers the cleanup of a bootstrap that fails before the follower index is ready to follow: the partial
 * restore must be removed rather than left behind holding the cluster RED.
 *
 * The race is opened by serialising the per-shard restores rather than throttling bandwidth (`chunk_size` is
 * clamped to `[1mb, 1gb]`, which a small index burns through instantly). The leader index gets many shards,
 * the follower gets `cluster.routing.allocation.node_initial_primaries_recoveries: 1` so only one shard
 * primary restores at a time, and the leader index is deleted right after `_start` returns. Shards that
 * haven't started restoring then fail at `getLeaderClusterState()` with `IndexNotFoundException`.
 *
 * Two levers that don't work, so they aren't retried:
 *  - `cluster.routing.allocation.enable: none` before `_start` hangs the call, because
 *    `addIndexReplicationMetadata(...)` writes to the `.replication-metadata-store` system index before the
 *    persistent task starts, and that index can't allocate with allocation disabled cluster-wide.
 *  - `chunk_size = 1kb` is rejected outright (`must be >= [1mb]`).
 */
@MultiClusterAnnotations.ClusterConfigurations(
    MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
    MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)
class BootstrapFailureIT : MultiClusterRestTestCase() {

    private val leaderIndexName = "bootstrap_failure_leader"
    private val followerIndexName = "bootstrap_failure_follower"

    fun `test failed bootstrap removes the partial restore and leaves the cluster healthy`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        try {
            // Serialise the per-shard restores and throttle each transfer, so that when the leader is deleted
            // most shards have not begun restoring yet. See the class doc for why this particular throttle.
            followerClient.setBootstrapThrottle(
                chunkSize = "1mb",
                maxConcurrentFileChunks = 1,
                initialPrimariesRecoveries = 1
            )

            val leaderSettings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 20)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.key, Long.MAX_VALUE)
                .build()
            assertThat(
                leaderClient.indices()
                    .create(CreateIndexRequest(leaderIndexName).settings(leaderSettings), RequestOptions.DEFAULT)
                    .isAcknowledged
            ).isTrue()
            // Real data, so each shard restore is an actual multi-chunk file transfer. An empty store takes a
            // different, non-representative shortcut in RemoteClusterRepository.
            IndexUtil.fillIndex(leaderClient, leaderIndexName, 2000, 500, 200)

            // wait_for_restore=false -> _start returns as soon as the task reaches RESTORING, leaving the
            // per-shard restores running in the background. waitForShardsInit=false -> do not block on shards
            // initializing, they never will here.
            followerClient.startReplication(
                StartReplicationRequest("source", leaderIndexName, followerIndexName),
                TimeValue.timeValueSeconds(60),
                waitForShardsInit = false,
                waitForRestore = false
            )

            // The race: remove the leader at the earliest possible moment, right after _start returns at
            // RESTORING. Deliberately not gated on the follower index existing first, since that check would
            // burn the very window being exploited.
            logger.info("_start returned at RESTORING; deleting leader index now to race the shard restores")
            assertThat(
                leaderClient.indices().delete(DeleteIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
                    .isAcknowledged
            ).isTrue()

            // The restore had already created the follower index before _start returned. If this is false the
            // reproduction did not reproduce and the assertions below would pass vacuously.
            assertThat(
                followerClient.indices().exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT)
            ).withFailMessage("follower index was never created by the restore").isTrue()

            // The bootstrap fails and markAsFailed() ends the state machine, so the task goes away.
            assertBusy({
                assertThat(followerClient.getIndexReplicationTask(followerIndexName))
                    .withFailMessage(
                        "index replication task is still present - the bootstrap either has not failed yet " +
                                "or the restore won the race. Increase the leader index size / lower " +
                                "chunk_size to widen the window."
                    )
                    .isEmpty()
            }, 240, TimeUnit.SECONDS)

            // cleanup() must have removed the partial restore. Without that, the half-restored index survives
            // with unassigned primaries and holds the cluster RED until an operator deletes it.
            assertBusy({
                assertThat(
                    followerClient.indices().exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT)
                )
                    .withFailMessage("the partially restored follower index was not removed after the bootstrap failed")
                    .isFalse()
            }, 120, TimeUnit.SECONDS)

            // With nothing orphaned, the follower cluster recovers on its own.
            assertBusy({
                val health = followerClient.cluster().health(ClusterHealthRequest(), RequestOptions.DEFAULT)
                logger.info(
                    "follower health: status=${health.status}, unassigned=${health.unassignedShards}, " +
                            "active=${health.activeShards}"
                )
                assertThat(health.status)
                    .withFailMessage("follower cluster did not recover after the bootstrap failure")
                    .isNotEqualTo(ClusterHealthStatus.RED)
            }, 120, TimeUnit.SECONDS)

            // And _start can be retried without any manual cleanup. Previously the orphaned index tripped the
            // "Cant use same index again for replication" guard and the retry failed with a 400.
            assertThat(
                leaderClient.indices()
                    .create(CreateIndexRequest(leaderIndexName).settings(leaderSettings), RequestOptions.DEFAULT)
                    .isAcknowledged
            ).isTrue()

            followerClient.startReplication(
                StartReplicationRequest("source", leaderIndexName, followerIndexName),
                TimeValue.timeValueSeconds(60),
                waitForShardsInit = false,
                waitForRestore = false
            )
        } finally {
            // Restore defaults and clean up, so a failure here does not cascade into later tests. The retry
            // above leaves replication running, so stop it before removing the indices underneath it.
            runCatching { followerClient.setBootstrapThrottle(null, null, null) }
            runCatching { followerClient.stopReplication(followerIndexName) }
            runCatching {
                followerClient.indices().delete(DeleteIndexRequest(followerIndexName), RequestOptions.DEFAULT)
            }
            runCatching {
                leaderClient.indices().delete(DeleteIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
            }
        }
    }

    /**
     * Throttles (or resets) the bootstrap restore on the follower. Pass all-null to reset to defaults.
     *
     * `initialPrimariesRecoveries` is the load-bearing one: it serialises snapshot-restore primary recoveries
     * so most shards have not started when the leader disappears. Transient rather than persistent on purpose,
     * so a leak cannot slow every subsequent test in the suite.
     */
    private fun RestHighLevelClient.setBootstrapThrottle(
        chunkSize: String?,
        maxConcurrentFileChunks: Int?,
        initialPrimariesRecoveries: Int?
    ) {
        val chunkSizeKey = "plugins.replication.follower.index.recovery.chunk_size"
        val parallelKey = "plugins.replication.follower.index.recovery.max_concurrent_file_chunks"
        val primariesKey = "cluster.routing.allocation.node_initial_primaries_recoveries"
        val builder = Settings.builder()
        if (chunkSize != null) builder.put(chunkSizeKey, chunkSize) else builder.putNull(chunkSizeKey)
        if (maxConcurrentFileChunks != null) {
            builder.put(parallelKey, maxConcurrentFileChunks)
        } else {
            builder.putNull(parallelKey)
        }
        if (initialPrimariesRecoveries != null) {
            builder.put(primariesKey, initialPrimariesRecoveries)
        } else {
            builder.putNull(primariesKey)
        }
        val request = ClusterUpdateSettingsRequest().transientSettings(builder.build())
        assertThat(cluster().putSettings(request, RequestOptions.DEFAULT).isAcknowledged).isTrue()
    }
}
