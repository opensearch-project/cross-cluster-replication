## Version 1.3.5.0 Release Notes

Compatible with OpenSearch 1.3.5

### Bug Fixes

* Fix for missing ShardReplicationTasks on new nodes ([#497](https://github.com/opensearch-project/cross-cluster-replication/pull/497))
* Updating filters as well during Alias update ([#491](https://github.com/opensearch-project/cross-cluster-replication/pull/491))
* Fixing the follower checkpoint updation in follower stats API ([#441](https://github.com/opensearch-project/cross-cluster-replication/pull/441))

### Enhancements
* Add lastExecutionTime for autofollow coroutine ([#508](https://github.com/opensearch-project/cross-cluster-replication/pull/508))

### Infrastructure
* Added build.sh compatible with 1.3.x release ([#483](https://github.com/opensearch-project/cross-cluster-replication/pull/483))
* Staging for version increment automation ([#466](https://github.com/opensearch-project/cross-cluster-replication/pull/466))
