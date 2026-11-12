## Version 3.10.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.10.0

### Enhancements

* Seek to chunk offset on the leader during bootstrap instead of read-and-discard, reducing leader-side I/O from O(N²) to O(N) per file ([#1751](https://github.com/opensearch-project/cross-cluster-replication/pull/1751))
* Coalesce concurrent `_stop` replication cluster-state updates via TaskBatcher to eliminate timeout failures at high concurrency ([#1770](https://github.com/opensearch-project/cross-cluster-replication/pull/1770))
* Make `FollowerClusterStats` thread-safe with `ConcurrentHashMap` and null-guard stat reads to prevent shard failures during concurrent task start/stop ([#1771](https://github.com/opensearch-project/cross-cluster-replication/pull/1771))
* Expose leader translog generation threshold as a configurable follower cluster setting (`plugins.replication.follower.leader_translog_generation_threshold_size`) to reduce leader CPU overhead ([#1766](https://github.com/opensearch-project/cross-cluster-replication/pull/1766))
* Refresh `follower_checkpoint` from the live shard on every read-loop iteration so `follower_stats` no longer reports stale checkpoints on idle indices ([#1745](https://github.com/opensearch-project/cross-cluster-replication/pull/1745))

### Bug Fixes

* Fix continuous replica churn on follower indices using `auto_expand_replicas` by guarding `number_of_replicas` removal in the metadata-sync path ([#1772](https://github.com/opensearch-project/cross-cluster-replication/pull/1772))
