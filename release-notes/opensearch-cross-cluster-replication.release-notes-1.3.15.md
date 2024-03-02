## Version 1.3.15 Release Notes

Compatible with OpenSearch 1.3.15

### Enhancement
* Support for indices clean-up after integ test runs ([#619] (https://github.com/opensearch-project/cross-cluster-replication/pull/619))
* Batch request handling for replication metadata update under cluster state ([#772] (https://github.com/opensearch-project/cross-cluster-replication/pull/772))
* Added retention lease with followerClusterUUID ([#864] (https://github.com/opensearch-project/cross-cluster-replication/pull/864))
* Modified autofollow stats to rely on single source for failed indices ([#708] (https://github.com/opensearch-project/cross-cluster-replication/pull/708))
* Modified autofollow retry scheduler logic to account for completed runs ([#839] (https://github.com/opensearch-project/cross-cluster-replication/pull/839))

### Bug Fixes
* Handled serialization issues with UpdateReplicationStateDetailsRequest ([#866] (https://github.com/opensearch-project/cross-cluster-replication/pull/866))
* Handled exception in getAssignment method which is used to assign shard replication task via persistent task cluster service ([#937] (https://github.com/opensearch-project/cross-cluster-replication/pull/937))
* Initialization of the leaderCheckpoint when ShardReplicationTask restarts on a new node and correctly handled retention lease renewal (if the lease already exists) during bootstrap ([#904] (https://github.com/opensearch-project/cross-cluster-replication/pull/904))
* Removal any stale replication tasks from cluster state ([#905] (https://github.com/opensearch-project/cross-cluster-replication/pull/905))
* Avoided use of indicesService in Resume replication flow ([#1030] (https://github.com/opensearch-project/cross-cluster-replication/pull/1030))


### Security Fixes
* Changed version of ipaddress library to 5.4.1 for OS 1.3([#1339] (https://github.com/opensearch-project/cross-cluster-replication/pull/1339))
