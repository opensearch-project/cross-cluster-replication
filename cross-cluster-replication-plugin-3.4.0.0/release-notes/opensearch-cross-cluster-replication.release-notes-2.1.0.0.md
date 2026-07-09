## Version 2.1.0.0 Release Notes

Compatible with OpenSearch 2.1.0

### Bug Fixes
* Bugfix: Validate leader index primary shard state before starting replication ([#419](https://github.com/opensearch-project/cross-cluster-replication/pull/419))
* [BUG] [Autofollow] Filter indices in recovery state from leader cluster before starting replication ([#341](https://github.com/opensearch-project/cross-cluster-replication/pull/341))
* Sync the mapping from leader index and retry for MapperParsingException ([#411](https://github.com/opensearch-project/cross-cluster-replication/pull/411))
* Added error logs for replication state transition to auto_pause or failed state with reason ([#408](https://github.com/opensearch-project/cross-cluster-replication/pull/408))

