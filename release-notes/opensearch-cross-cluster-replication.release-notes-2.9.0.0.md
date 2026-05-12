## Version 2.9.0.0 Release Notes

Compatible with OpenSearch 2.9.0


### Bug Fixes
* Handle bug in Shard replication task assignment ([#881](https://github.com/opensearch-project/cross-cluster-replication/pull/881))
* Update Leader checkpoint when shard replication task is reinitialized ([#904](https://github.com/opensearch-project/cross-cluster-replication/pull/904))
* Correctly handle retention lease renewal (if the lease already exists) during bootstrap ([#904](https://github.com/opensearch-project/cross-cluster-replication/pull/904))
* Clear persistent tasks from cluster state after STOP API is triggered ([#905](https://github.com/opensearch-project/cross-cluster-replication/pull/905))
* Handle OpenSearchRejectExecuteException Exception during replay ([#1004](https://github.com/opensearch-project/cross-cluster-replication/pull/1004))
* Fix Resume replication flow in dedicated master node configuration ([#1030](https://github.com/opensearch-project/cross-cluster-replication/pull/1030))