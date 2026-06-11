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
import org.opensearch.persistent.PersistentTasksCustomMetadata
import org.opensearch.replication.ReplicationSettings
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.metadata.ReplicationOverallState
import org.opensearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import org.opensearch.replication.metadata.state.ReplicationStateMetadata
import org.opensearch.replication.task.index.IndexReplicationExecutor
import org.opensearch.replication.task.index.IndexReplicationParams
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
        // Don't reference clusterService.localNode() here — at plugin createComponents time the cluster state
        // has not yet been initialized and accessing it throws. The first clusterChanged event will surface
        // local node identity in subsequent log lines via reconcile().
        log.info("NodeReplicationController started")
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
        val desired = computeDesiredShards(event.state())

        // Stop contexts for shards no longer desired (relocated away, paused, stopped, deleted).
        val toStop = contexts.keys - desired.keys
        for (shardId in toStop) {
            val ctx = contexts.remove(shardId) ?: continue
            log.info("Stopping replication context for $shardId (no longer eligible)")
            ctx.stop()
        }

        // Start contexts for newly-desired shards.
        for ((shardId, params) in desired) {
            if (contexts.containsKey(shardId)) continue
            val ctx = ShardReplicationContext(
                leaderAlias = params.leaderAlias,
                leaderShardId = params.leaderShardId,
                followerShardId = shardId,
                clusterService = clusterService,
                threadPool = threadPool,
                client = client,
                replicationMetadataManager = replicationMetadataManager,
                replicationSettings = replicationSettings,
                followerClusterStats = followerClusterStats,
            )
            ctx.start(controllerScope)
            contexts[shardId] = ctx
            log.info("Started replication context for $shardId (leader=${params.leaderAlias}${params.leaderShardId})")
        }
    }

    /**
     * Pure function over cluster state — extracted for testability. Returns the set of follower primary shards
     * assigned to the local node where the corresponding index is in RUNNING replication state, mapped to the
     * params needed to start a [ShardReplicationContext].
     *
     * Leader alias and leader-index identity (name + UUID) are sourced from the [IndexReplicationParams] of the
     * still-running per-index persistent task. The follower index settings only carry the leader index name, not
     * the cluster alias or the leader index UUID.
     */
    internal fun computeDesiredShards(state: org.opensearch.cluster.ClusterState): Map<ShardId, DesiredShardParams> {
        val localNodeId = state.nodes().localNodeId ?: return emptyMap()

        val replicationStateMd = state.metadata().custom<ReplicationStateMetadata>(ReplicationStateMetadata.NAME)
            ?: return emptyMap()
        val runningIndices = replicationStateMd.replicationDetails
            .filterValues { params -> params[REPLICATION_LAST_KNOWN_OVERALL_STATE] == ReplicationOverallState.RUNNING.name }
            .keys
        if (runningIndices.isEmpty()) return emptyMap()

        // Build a map of follower-index-name -> IndexReplicationParams by scanning persistent tasks. The index
        // task is still a persistent task; this is how we recover the leader alias and leader Index (name+UUID).
        val persistentTasks = state.metadata().custom<PersistentTasksCustomMetadata>(PersistentTasksCustomMetadata.TYPE)
        val indexParamsByFollower: Map<String, IndexReplicationParams> = persistentTasks?.tasks()
            ?.filter { it.taskName == IndexReplicationExecutor.TASK_NAME }
            ?.mapNotNull { task ->
                @Suppress("UNCHECKED_CAST")
                val params = task.params as? IndexReplicationParams ?: return@mapNotNull null
                params.followerIndexName to params
            }
            ?.toMap()
            ?: emptyMap()

        val result = mutableMapOf<ShardId, DesiredShardParams>()
        for (indexName in runningIndices) {
            val indexParams = indexParamsByFollower[indexName] ?: continue
            val indexRouting = state.routingTable().index(indexName) ?: continue

            for (shardEntry in indexRouting.shards) {
                val primary: ShardRouting? = shardEntry.value.primaryShard()
                if (primary == null || !primary.started()) continue
                if (primary.currentNodeId() != localNodeId) continue

                val followerShardId = primary.shardId()
                val leaderShardId = ShardId(indexParams.leaderIndex, followerShardId.id)
                result[followerShardId] = DesiredShardParams(indexParams.leaderAlias, leaderShardId)
            }
        }
        return result
    }

    internal data class DesiredShardParams(val leaderAlias: String, val leaderShardId: ShardId)

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
