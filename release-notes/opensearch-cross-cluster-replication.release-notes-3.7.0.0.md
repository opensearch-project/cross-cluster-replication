## Version 3.7.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.7.0

### Features

* Add support for clearing stale persistent tasks in Stop/Pause/Start/Resume APIs to prevent orphaned tasks from blocking replication operations ([#1629](https://github.com/opensearch-project/cross-cluster-replication/pull/1629))
* Add `cluster_manager_timeout` parameter support for all cross-cluster replication REST APIs ([#1638](https://github.com/opensearch-project/cross-cluster-replication/pull/1638))

### Enhancements

* Add diagnostic logging to improve troubleshooting of CCR replication failures ([#1659](https://github.com/opensearch-project/cross-cluster-replication/pull/1659))
* Onboard code diff analyzer/reviewer and issue deduplication workflows ([#1665](https://github.com/opensearch-project/cross-cluster-replication/pull/1665))

### Bug Fixes

* Fix `isRemoteEnabledOrMigrating` to correctly detect remote-store clusters, resolving ~5 minute replication delays caused by stale checkpoint reads ([#1688](https://github.com/opensearch-project/cross-cluster-replication/pull/1688))
* Skip syncing `number_of_replicas` when follower has `auto_expand_replicas` active to prevent perpetual yellow cluster state ([#1664](https://github.com/opensearch-project/cross-cluster-replication/pull/1664))
* Fix security plugin compatibility for 3.7.0 by implementing `TransportIndicesResolvingAction` on metadata update action ([#1667](https://github.com/opensearch-project/cross-cluster-replication/pull/1667))
