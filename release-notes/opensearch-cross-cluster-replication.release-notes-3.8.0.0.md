## Version 3.8.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.8.0

### Features

* Add bulk feature support for all replication APIs, enabling start, stop, pause, and resume operations on multiple indexes matching a pattern with a single API call ([#1699](https://github.com/opensearch-project/cross-cluster-replication/pull/1699))
* Add optional `follower_index_pattern` support to autofollow API, allowing follower index renaming using a `{{leader_index}}` placeholder to avoid name collisions ([#1705](https://github.com/opensearch-project/cross-cluster-replication/pull/1705))

### Bug Fixes

* Fix stale and negative counters in the follower_stats API by deriving sync state from cluster metadata cross-referenced with live shard tasks ([#1717](https://github.com/opensearch-project/cross-cluster-replication/pull/1717))

### Infrastructure

* Onboard new backport-pr reusable GitHub workflow for cross-cluster-replication ([#1710](https://github.com/opensearch-project/cross-cluster-replication/pull/1710))
* Pin all GitHub Actions to full-length commit SHAs for supply-chain security compliance ([#1706](https://github.com/opensearch-project/cross-cluster-replication/pull/1706))
