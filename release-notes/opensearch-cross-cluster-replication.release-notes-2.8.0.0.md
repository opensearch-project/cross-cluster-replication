## Version 2.8.0.0 Release Notes

Compatible with OpenSearch 2.8.0


### Enhancements
* Support CCR for k-NN enabled indices ([#760](https://github.com/opensearch-project/cross-cluster-replication/pull/760))

### Bug Fixes
* Handle serialization issues with UpdateReplicationStateDetailsRequest ([#866](https://github.com/opensearch-project/cross-cluster-replication/pull/866))
* Two followers using same remote alias can result in replication being auto-paused ([#833](https://github.com/opensearch-project/cross-cluster-replication/pull/833))
