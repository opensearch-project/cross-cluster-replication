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

package org.opensearch.replication.action.bulk

import org.opensearch.action.support.ActionFilters
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.replication.ReplicationSettings
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.task.bulk.BulkOperationType
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client

class TransportBulkStartReplicationAction @Inject constructor(
    transportService: TransportService, actionFilters: ActionFilters,
    client: Client, clusterService: ClusterService, threadPool: ThreadPool,
    indexNameExpressionResolver: IndexNameExpressionResolver,
    replicationMetadataManager: ReplicationMetadataManager,
    replicationSettings: ReplicationSettings
) : TransportBulkReplicationAction(
    BulkStartReplicationAction.NAME, BulkOperationType.START,
    transportService, actionFilters, client, clusterService, threadPool,
    indexNameExpressionResolver, replicationMetadataManager, replicationSettings
)

class TransportBulkStopReplicationAction @Inject constructor(
    transportService: TransportService, actionFilters: ActionFilters,
    client: Client, clusterService: ClusterService, threadPool: ThreadPool,
    indexNameExpressionResolver: IndexNameExpressionResolver,
    replicationMetadataManager: ReplicationMetadataManager,
    replicationSettings: ReplicationSettings
) : TransportBulkReplicationAction(
    BulkStopReplicationAction.NAME, BulkOperationType.STOP,
    transportService, actionFilters, client, clusterService, threadPool,
    indexNameExpressionResolver, replicationMetadataManager, replicationSettings
)

class TransportBulkPauseReplicationAction @Inject constructor(
    transportService: TransportService, actionFilters: ActionFilters,
    client: Client, clusterService: ClusterService, threadPool: ThreadPool,
    indexNameExpressionResolver: IndexNameExpressionResolver,
    replicationMetadataManager: ReplicationMetadataManager,
    replicationSettings: ReplicationSettings
) : TransportBulkReplicationAction(
    BulkPauseReplicationAction.NAME, BulkOperationType.PAUSE,
    transportService, actionFilters, client, clusterService, threadPool,
    indexNameExpressionResolver, replicationMetadataManager, replicationSettings
)

class TransportBulkResumeReplicationAction @Inject constructor(
    transportService: TransportService, actionFilters: ActionFilters,
    client: Client, clusterService: ClusterService, threadPool: ThreadPool,
    indexNameExpressionResolver: IndexNameExpressionResolver,
    replicationMetadataManager: ReplicationMetadataManager,
    replicationSettings: ReplicationSettings
) : TransportBulkReplicationAction(
    BulkResumeReplicationAction.NAME, BulkOperationType.RESUME,
    transportService, actionFilters, client, clusterService, threadPool,
    indexNameExpressionResolver, replicationMetadataManager, replicationSettings
)
