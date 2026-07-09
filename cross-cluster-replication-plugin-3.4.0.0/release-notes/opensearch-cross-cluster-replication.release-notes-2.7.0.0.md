## Version 2.7.0.0 Release Notes

Compatible with OpenSearch 2.7.0

### Bug Fixes
* Modified autofollow stats to rely on single source for failed indices ([#736](https://github.com/opensearch-project/cross-cluster-replication/pull/736))
* Update UpdateAutoFollowPatternIT "test auto follow stats" to wait for 60 seconds ([#745](https://github.com/opensearch-project/cross-cluster-replication/pull/745))
* Update imports from common to core package ([#761](https://github.com/opensearch-project/cross-cluster-replication/pull/761))
* Adding a proxy mode connection setup for CCR ([#795](https://github.com/opensearch-project/cross-cluster-replication/pull/795))
* Handled exception under multi-field mapping update ([#789](https://github.com/opensearch-project/cross-cluster-replication/pull/789))
* Handled batch requests for replication metadata update under cluster state ([#778](https://github.com/opensearch-project/cross-cluster-replication/pull/778))

### Infrastructure
* Added support for running Integtest on Remote clusters ([#733](https://github.com/opensearch-project/cross-cluster-replication/pull/733))
