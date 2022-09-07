## Version 2.3.0.0 Release Notes

Compatible with OpenSearch 2.3.0

### Bug Fixes
* Updating filters as well during Alias update ([#491](https://github.com/opensearch-project/cross-cluster-replication/pull/491))
* Modified _stop replication API to remove any stale replication settings on existing index ([#410](https://github.com/opensearch-project/cross-cluster-replication/pull/410))
* Fix for missing ShardReplicationTasks on new nodes ([#497](https://github.com/opensearch-project/cross-cluster-replication/pull/497))
* For segrep enabled indices, use NRTReplicationEngine for replica shards ([#486](https://github.com/opensearch-project/cross-cluster-replication/pull/486))

### Enhancements
* Add lastExecutionTime for autofollow coroutine ([#508](https://github.com/opensearch-project/cross-cluster-replication/pull/508))
* Modified security artifacts to fetch from latest build version ([#474](https://github.com/opensearch-project/cross-cluster-replication/pull/474))
* add updateVersion task ([#489](https://github.com/opensearch-project/cross-cluster-replication/pull/489))
* Bumped snakeyaml version to address CVE-2022-25857 ([#540](https://github.com/opensearch-project/cross-cluster-replication/pull/540))