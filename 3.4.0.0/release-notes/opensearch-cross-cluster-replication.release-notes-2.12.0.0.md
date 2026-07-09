## Version 2.12.0 Release Notes

Compatible with OpenSearch 2.12.0

## Bug Fixes

* Implement getSystemIndexDescriptors to support SystemIndex for replication plugin ([#1290](https://github.com/opensearch-project/cross-cluster-replication/pull/1290))
* Correct error message including what fields are missing when field are not passed when starting replication ([#1292](https://github.com/opensearch-project/cross-cluster-replication/pull/1292))
* Ignoring all the final settings to copy from leader to follower as those settings won't be able to apply as those are not updatable ([#1304](https://github.com/opensearch-project/cross-cluster-replication/pull/1304))

## Maintenance
* Updates integTest behavior to accept the version and set the password accordingly, removes admin:admin references and force HTTP1 policy for local clusters([#1298](https://github.com/opensearch-project/cross-cluster-replication/pull/1298))([#1318](https://github.com/opensearch-project/cross-cluster-replication/pull/1318))
