## Version 1.2.0.0 Release Notes

Compatible with OpenSearch 1.2.0
### Enhancements
* Added support for start replication block ([#250](https://github.com/opensearch-project/cross-cluster-replication/pull/250))
* Handled case for _start replication with leader cluster at higher version than the follower cluster ([#247](https://github.com/opensearch-project/cross-cluster-replication/pull/247))
* Handled stop replication when remote cluster is removed ([#241](https://github.com/opensearch-project/cross-cluster-replication/pull/241))
* Filtered out replication exceptions from retrying ([#238](https://github.com/opensearch-project/cross-cluster-replication/pull/238))
* Using time out in cluster state observer as we are reusing the observer ([#215](https://github.com/opensearch-project/cross-cluster-replication/pull/215))
* Renewing retention lease with global-ckp + 1 , as we want operations from that seq number ([#206](https://github.com/opensearch-project/cross-cluster-replication/pull/206))
* Derive security context information when security plugin fails to populate user info ([#204](https://github.com/opensearch-project/cross-cluster-replication/pull/204))
* Provide a custom TranslogDeletionPolicy for translog pruning based on retention leases ([#209](https://github.com/opensearch-project/cross-cluster-replication/pull/209))

### Bug Fixes
* Bugfix: Refactored replication specific translog policy and addressed falky tests ([#236](https://github.com/opensearch-project/cross-cluster-replication/pull/236))
* Bugfix: Rename translog pruning setting to match 1.1 convention and addressed behavior for translog deletion to be same as 1.1 ([#234](https://github.com/opensearch-project/cross-cluster-replication/pull/234))
* BugFix: Changes to ensure replication tasks are not failing prematurely ([#231](https://github.com/opensearch-project/cross-cluster-replication/pull/231))
* Bugfix: Replication task fails to initialize due to state parsing failure ([#226](https://github.com/opensearch-project/cross-cluster-replication/pull/226))

### Infrastructure
* [CI] Version bump to 1.2 ([#213](https://github.com/opensearch-project/cross-cluster-replication/pull/213))
* Add DCO chek workflow ([#212](https://github.com/opensearch-project/cross-cluster-replication/pull/212))
