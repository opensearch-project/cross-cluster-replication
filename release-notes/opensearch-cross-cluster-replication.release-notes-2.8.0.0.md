## Version 2.7.0.0 Release Notes

Compatible with OpenSearch 2.8.0


### Enhancements
* Support CCR for k-NN enabled indices ([#650](https://github.com/opensearch-project/cross-cluster-replication/issues/650))

### Bug Fixes
* Replication stops with NotSerializableExceptionWrapper exception ([#627](https://github.com/opensearch-project/cross-cluster-replication/issues/627))
* Two followers using same remote alias can result in replication being auto-paused ([#554](https://github.com/opensearch-project/cross-cluster-replication/issues/554))
