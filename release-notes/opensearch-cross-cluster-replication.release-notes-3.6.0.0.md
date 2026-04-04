## Version 3.6.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.6.0

### Bug Fixes

* Fix typo in `validFileNameExcludingAsterisk` to align with core OpenSearch fix ([#1639](https://github.com/opensearch-project/cross-cluster-replication/pull/1639))
* Fix ReplicationEngine to construct lightweight replica-origin copies of Index/Delete operations for non-primary planning path, resolving compilation and assertion failures after core refactor ([#1647](https://github.com/opensearch-project/cross-cluster-replication/pull/1647))

### Maintenance

* Upgrade filelock to 3.20.3 to address CVE-2025-68146 and CVE-2026-22701 race condition vulnerabilities ([#1637](https://github.com/opensearch-project/cross-cluster-replication/pull/1637))
* Fix CVE-2026-25645 and CVE-2026-24400 ([#1650](https://github.com/opensearch-project/cross-cluster-replication/pull/1650))
