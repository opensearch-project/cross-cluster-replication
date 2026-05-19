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

package org.opensearch.replication.task.shard

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.routing.ShardRouting
import org.opensearch.cluster.service.ClusterService
import org.opensearch.core.index.shard.ShardId
import org.opensearch.replication.ReplicationSettings
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.metadata.ReplicationOverallState
import org.opensearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import org.opensearch.replication.metadata.state.ReplicationStateMetadata
import org.opensearch.replication.util.stackTraceToString
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

/**
 * Per-node controller that manages [ShardReplicationContext] instances for follower primary
 * shards on this node. Replaces the persistent-task-based [ShardReplicationTask] lifecycle.
 *
 * Listens to [ClusterChangedEvent]s and reconciles the set of running contexts against:
 *   - the local routing assignments (which follower primary shards live on this node), and
 *   - the replication state metadata (whether each index is in RUNNING state).
 *
 * Also runs a periodic timer to renew retention leases for idle shards — closing the failure
 * mode where a shard with no traffic stops renewing its lease via the replicate loop and
 * eventually has its lease expire.
 */
@ObsoleteCoroutinesApi
class NodeReplicationController(
    private val clusterService: ClusterService,
    private val threadPool: ThreadPool,
    private val client: Client,
    private val replicationMetadataManager: ReplicationMetadataManager,
    private val replicationSettings: ReplicationSettings,
    private val followerClusterStats: FollowerClusterStats,
) : ClusterStateListener {

    companion object {
        private val log = LogManager.getLogger(NodeReplicationController::class.java)

        // Idle-lease sweep frequency. Configurable via setting in a follow-up; defaults are sensible for the
        // OpenSearch lease default expiry of 12h.
        private val IDLE_LEASE_SWEEP_INTERVAL = TimeUnit.MINUTES.toMillis(30)
        private val IDLE_LEASE_THRESHOLD = TimeUnit.HOURS.toMillis(2)
    }

    private val contexts = ConcurrentHashMap<ShardId, ShardReplicationContext>()

    // Single-threaded reconciliation. Cluster state events can land in rapid succession; serializing reconcile
    // work avoids races between concurrent start/stop decisions for the same shard.
    private val supervisorJob = SupervisorJob()
    private val controllerScope = CoroutineScope(supervisorJob + Dispatchers.Default)

    private var idleLeaseSweep: Scheduler.Cancellable? = null

    fun start() {
        clusterService.addListener(this)
        idleLeaseSweep = threadPool.scheduleWithFixedDelay(
            { runIdleLeaseSweep() },
            org.opensearch.common.unit.TimeValue.timeValueMillis(IDLE_LEASE_SWEEP_INTERVAL),
            ThreadPool.Names.GENERIC
        )
        log.info("NodeReplicationController started on node ${clusterService.localNode().id}")
    }

    fun stop() {
        clusterService.removeListener(this)
        idleLeaseSweep?.cancel()
        contexts.values.forEach { it.stop() }
        contexts.clear()
        controllerScope.cancel()
    }

    override fun clusterChanged(event: ClusterChangedEvent) {
        // Reconcile asynchronously to avoid holding up the cluster state applier thread.
        controllerScope.launch {
            try {
                reconcile(event)
            } catch (e: Throwable) {
                log.warn("NodeReplicationController reconcile failed: ${e.stackTraceToString()}")
            }
        }
    }

    @Synchronized
    private fun reconcile(event: ClusterChangedEvent) {
        val state = event.state()
        val localNodeId = state.nodes().localNodeId ?: return

        // Map of replicated indices -> their leader alias. Empty if no replication metadata at all.
        val replicationStateMd = state.metadata().custom<ReplicationStateMetadata>(ReplicationStateMetadata.NAME)
        val replicatedIndices: Map<String, String> = replicationStateMd?.replicationDetails
            ?.filterValues { params -> params[REPLICATION_LAST_KNOWN_OVERALL_STATE] == ReplicationOverallState.RUNNING.name }
            ?.mapValues { /* leader alias not directly in state params; resolved per-shard below */ "" }
            ?: emptyMap()

        // Compute the set of follower primary shards assigned locally for indices that are in RUNNING state.
        // For each, we need leader alias + leader shard id from the index metadata.
        val desired = mutableMapOf<ShardId, ShardReplicationContext.() -> Unit>() // placeholder map
        val newContexts = mutableMapOf<ShardId, ShardReplicationContext>()

        for ((indexName, _) in replicatedIndices) {
            val indexRouting = state.routingTable().index(indexName) ?: continue
            val indexMetadata = state.metadata().index(indexName) ?: continue

            // Read leader alias and leader index UUID from the follower index settings (set during bootstrap).
            val leaderAlias = indexMetadata.settings.get("index.plugins.replication.followed_by")
                ?: indexMetadata.settings.get("index.plugins.replication.leader_cluster_alias")
                ?: continue
            val leaderIndexName = indexMetadata.settings.get("index.plugins.replication.leader_index_name")
                ?: indexMetadata.settings.get("index.plugins.replication.leader_index")
                ?: indexName
            val leaderIndexUUID = indexMetadata.settings.get("index.plugins.replication.leader_index_uuid")
                ?: indexMetadata.index.uuid

            for (shardEntry in indexRouting.shards) {
                val shardRoutingTable = shardEntry.value
                val primary: ShardRouting? = shardRoutingTable.primaryShard()
                if (primary == null || !primary.started()) continue
                if (primary.currentNodeId() != localNodeId) continue

                val followerShardId = primary.shardId()
                val leaderShardId = ShardId(
                    org.opensearch.core.index.Index(leaderIndexName, leaderIndexUUID),
                    followerShardId.id
                )

                val ctx = contexts[followerShardId] ?: ShardReplicationContext(
                    leaderAlias = leaderAlias,
                    leaderShardId = leaderShardId,
                    followerShardId = followerShardId,
                    clusterService = clusterService,
                    threadPool = threadPool,
                    client = client,
                    replicationMetadataManager = replicationMetadataManager,
                    replicationSettings = replicationSettings,
                    followerClusterStats = followerClusterStats,
                ).also {
                    it.start(controllerScope)
                    log.info("Started replication context for $followerShardId (leader=$leaderAlias$leaderShardId)")
                }
                newContexts[followerShardId] = ctx
            }
        }

        // Stop contexts for shards no longer in the desired set (relocated away, paused, stopped, deleted).
        val toStop = contexts.keys - newContexts.keys
        for (shardId in toStop) {
            val ctx = contexts.remove(shardId) ?: continue
            log.info("Stopping replication context for $shardId (no longer eligible)")
            ctx.stop()
        }

        // Atomically swap the active map. Existing entries in newContexts are reused refs; new ones are added.
        for ((shardId, ctx) in newContexts) {
            contexts.putIfAbsent(shardId, ctx)
        }
    }

    /**
     * Periodic sweep over tracked contexts to renew leases that haven't been renewed recently. For active
     * contexts this is a no-op (their replicate loop renews on every batch). For idle shards (no traffic),
     * this is the only path that keeps the lease alive.
     */
    private fun runIdleLeaseSweep() {
        controllerScope.launch {
            for (ctx in contexts.values) {
                try {
                    ctx.renewLeaseIfStale(IDLE_LEASE_THRESHOLD)
                } catch (e: Exception) {
                    log.warn("Idle-lease sweep failed for ${ctx.followerShardId}: ${e.message}")
                }
            }
        }
    }

    // Test/observation accessor. Not exposed via REST in this PR; reserved for the follow-up status API.
    fun trackedShards(): Set<ShardId> = contexts.keys.toSet()
}
