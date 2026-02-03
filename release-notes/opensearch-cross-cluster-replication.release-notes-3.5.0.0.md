## Version 3.5.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.5.0

### Bug Fixes
* Fix write index conflicts in bidirectional replication ([#1625](https://github.com/opensearch-project/cross-cluster-replication/pull/1625))
* Index Reopen Call should not be sent when index replication task restarts if the index is already open ([#1619](https://github.com/opensearch-project/cross-cluster-replication/pull/1619))
* Build failure fix for 3.5 ([#1624](https://github.com/opensearch-project/cross-cluster-replication/pull/1624))

### Infrastructure
* [Flaky Test] Attempt to fix flaky test by allowing 500 error on stopAllReplication for MultiNode tests ([#1630](https://github.com/opensearch-project/cross-cluster-replication/pull/1630))

### Maintenance
* Fix replication tests and increment version to 3.5.0 ([#1621](https://github.com/opensearch-project/cross-cluster-replication/pull/1621))