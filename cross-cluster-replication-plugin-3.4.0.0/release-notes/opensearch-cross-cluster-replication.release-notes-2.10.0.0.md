## Version 2.10.0.0 Release Notes

Compatible with OpenSearch 2.10.0


### Bug Fixes
* Settings are synced before syncing mapping ([#994](https://github.com/opensearch-project/cross-cluster-replication/pull/994))
* Handled OpenSearchRejectExecuteException, introduced new setting ```plugins.replication.follower.concurrent_writers_per_shard```. ([#1004](https://github.com/opensearch-project/cross-cluster-replication/pull/1004))
* Fixed tests relying on wait_for_active_shards, fixed test for single Node and consume numNodes ([#1091](https://github.com/opensearch-project/cross-cluster-replication/pull/1091))
* Excessive logging avoided during certain exception types such as OpensearchTimeoutException ([#1114](https://github.com/opensearch-project/cross-cluster-replication/pull/1114))
