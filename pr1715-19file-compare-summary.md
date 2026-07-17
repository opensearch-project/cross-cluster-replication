# PR #1715 - 19 File Comparison Summary

Compared:
- Workspace: `/Users/g0c01kb/Downloads/new_codebase/opensource/cross-cluster-replication/cross-cluster-replication-plugin-3.4.0.0`
- Reference: `/Users/g0c01kb/Downloads/cross-cluster-replication-3.4.0.0-branch`

PR file list extracted from: `/tmp/pr1715.diff`

## Counts
- `MATCH`: 1
- `DIFF`: 17
- `MISSING_IN_WORKSPACE`: 1

## Per-file status
- `MISSING_IN_WORKSPACE` `CCR-Fix-Summary.md`
- `DIFF` `src/main/kotlin/org/opensearch/replication/ReplicationEngine.kt`
- `DIFF` `src/main/kotlin/org/opensearch/replication/ReplicationPlugin.kt`
- `DIFF` `src/main/kotlin/org/opensearch/replication/action/autofollow/TransportAutoFollowClusterManagerNodeAction.kt`
- `MATCH` `src/main/kotlin/org/opensearch/replication/action/autofollow/UpdateAutoFollowPatternRequest.kt`
- `DIFF` `src/main/kotlin/org/opensearch/replication/action/index/ReplicateIndexRequest.kt`
- `DIFF` `src/main/kotlin/org/opensearch/replication/action/index/TransportReplicateIndexAction.kt`
- `DIFF` `src/main/kotlin/org/opensearch/replication/action/index/TransportReplicateIndexClusterManagerNodeAction.kt`
- `DIFF` `src/main/kotlin/org/opensearch/replication/metadata/ReplicationMetadataManager.kt`
- `DIFF` `src/main/kotlin/org/opensearch/replication/metadata/store/ReplicationMetadata.kt`
- `DIFF` `src/main/kotlin/org/opensearch/replication/metadata/store/ReplicationMetadataStore.kt`
- `DIFF` `src/main/kotlin/org/opensearch/replication/repository/RemoteClusterRepository.kt`
- `DIFF` `src/main/kotlin/org/opensearch/replication/seqno/RemoteClusterRetentionLeaseHelper.kt`
- `DIFF` `src/main/kotlin/org/opensearch/replication/task/autofollow/AutoFollowTask.kt`
- `DIFF` `src/main/kotlin/org/opensearch/replication/task/index/IndexReplicationExecutor.kt`
- `DIFF` `src/main/kotlin/org/opensearch/replication/task/index/IndexReplicationTask.kt`
- `DIFF` `src/main/kotlin/org/opensearch/replication/task/shard/ShardReplicationTask.kt`
- `DIFF` `src/main/kotlin/org/opensearch/replication/util/ValidationUtil.kt`
- `DIFF` `src/main/resources/mappings/replication-metadata-store.json`

