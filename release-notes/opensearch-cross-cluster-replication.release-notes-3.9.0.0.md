## Version 3.9.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.9.0

### Bug Fixes

* Fix replication stall when dynamic batch-size reduction was not applied to replayed (missing) batches, causing follower checkpoint to wedge under parallel readers ([#1746](https://github.com/opensearch-project/cross-cluster-replication/pull/1746))
* Fix data loss during bootstrap when in-flight writes caused sequence number gaps that were incorrectly filled with no-ops by the replication engine ([#1735](https://github.com/opensearch-project/cross-cluster-replication/pull/1735))
* Fix `removeStaleTasksForIndex` incorrectly deleting actively-running replication tasks when a transient `ListTasks` query failure was treated as "no tasks running" ([#1725](https://github.com/opensearch-project/cross-cluster-replication/pull/1725))
* Fix stop replication failing for restored follower indices that carry replication settings but lack a metadata document, leaving the index in a partially mutated state ([#1740](https://github.com/opensearch-project/cross-cluster-replication/pull/1740))
* Make autofollow pattern REMOVE idempotent so that double-removes or retries after partial removal no longer fail or orphan pattern metadata ([#1739](https://github.com/opensearch-project/cross-cluster-replication/pull/1739))
* Version-gate `FailedState` `errorMsg` serialization to prevent stream corruption during mixed-cluster rolling upgrades between pre-3.7 and 3.7+ nodes ([#1749](https://github.com/opensearch-project/cross-cluster-replication/pull/1749))

### Infrastructure

* Fix code coverage upload action ([#1750](https://github.com/opensearch-project/cross-cluster-replication/pull/1750))
