## Version 1.1.0.0 Release Notes

Compatible with OpenSearch 1.1.0

###Features
- Initial commit to support replication
- Porting replication plugin to OpenSearch
- Pause and Resume API (#10)
- Leader Stats API (#122)
- Follower stats API (#126)
- AutoFollow Stats API Addition (#131)


###Enhancements
- Configure the threadpool for leader cluster
- Renew retention lease with the last known synced checkpoint
- Handling node disconnections and shard movement
- Enforce metadata write block on follower indices
- System index support for replication metadata Added replication manager, Support for storing security metadata and Integration with security plugin 
- Fix for race condition with snapshot restore
- Status api changes.
- Changed names for assume roles to remote and local from leader and follower Added preference to fetch the metdata from the primary shard Additional logging for the metadata store
- Handling leader cluster disconnection failure scenarios during bootstrap (#17)
- Support for translog fetch at the leader(remote) cluster and enable translog pruning during replication setup
- Added random active shard selector for the getchanges operations
- Settings and Alias replication (#22)
- Rename opendistro endpoints
- Added connections alias and doc count in status api.
- Bug fixes related to security roles
- Bootstrapping bug fix and few integ tests.
- Adding validation on index settings (#37)
- Renamed connection alias and changed exception handling.
- Added non verbose version of status api.
- Added replication specific settings for recovery
- Add support for parallel reader and writer for ShardReplicationTask
- Changes to fail chain replication.
- Using non null RestoreInProgress value in case of no restore in progress (#54)
- Not allowing index starting with . for replication
- Changing the default recovery chunk size to 10MB
- Added reason to the replication metadata store
- Propagate shard failures to index task and auto-pause on failures.
- Propgate 4xx failures from ShardReplicationTask.
- Bug fixes and autofollow task resiliency (#59)
- Added permission validation steps at user level and role level
- Async metadata update. Separated metadata reader and updater (#52)
- Validate analyzer path when starting/resuming replication
- Prevent starting replication directly on the leader index alias. (#66)
- Change to stop showing replication related metrics in case of non running (syncing and bootstrap) state.
- Integ test for follower index deletion
- Adding `reason` while serializing PauseReplication Request (#71)
- Add integ test to verify mapping propagation
- Integ test to verify that delete/index operation are blocked on follower
- Changes to support security plugin integ tests
- Integ test: Verify that replication is paused when remote index is deleted
- added few integ tests when open and close is triggered on leader index (#77)
- Integ test : Stop replication when remote cluster is unreachable
- Moved validation checks after setup checks
- Modified tests to use custom roles for default tests
- Replication engine tests: Index operations on leader
- Add Handling of IndexReplicationTask cancellation and corresponding ITs (#87)
- Updated security plugin with latest changes
- Integ test: forcemerge and snapshot on leader during bootstrap (#76)
- Validate that soft delete settings is enabled at leader
- Adding Security for replication APIs and IT for DLS, FLS or FieldMasking … (#90)
- changed few naming conventions. 1. remote to leader, 2. local to follower, etc
- Gradle changes for OpenSearch
- Refactor code for OpenSearch
- Port terminology from OpenDistroForElasticsearch to OpenSearch. (#98)
- Changed the dependency to OS 1.1
- Rename the replication settings (#103)
- Support for mapping update for the replication store system index
- Adding UTs for IndexReplicationTask (#109)
- OS - Integ tests for reroutes
- Added additional comments for the node-level events
- Adding exp backoff for replay and get changes (#135)
- Rename to useroles in all the request payloads instead of assumeroles
- Initializing shard metric for UT (#141)
- Changing the log level to debug as this information is surfaced in stats
- Pause API should allow reason to be specified in the REST call. (#151)
- Verifying shard tasks are up in autofollow test (#153)
- Correcting param name for leader API (#163)
- Populate "AutoPaused: " prefix in the pause-reason while auto-pausing. (#170)

###Bug Fixes
- Fix submitClusterStateUpdateTask source
- Throw exception if replication is not enabled on the remote domain (#86)
- Increase wait time for snapshot/forcemerge integ tests
- Add validation on index name (#92)
- Remove opendistro security zip (#95)
- Handled all related exceptions for translog fetch
- Bugfix: Changes to restore default threadcontext after co-routine is suspended
- Remove packaged example as replication plugin will ship with OpenSearch. (#107)
- Bugfix: Handled errors during test role update (#112)
- Ignore forbidden APIs to access private members of ES datastructure (#120)
- [Bug] Starting missing shard tasks
- BugFix: Act on index remove events based index state for index replication task
- Bugfix: Modified index block before creation of shard tasks to avoid race conditions
- Bugfix: Rebase issue - Test clusters to default to 2 nodes
- Fix for RetentionLeaseInvalidRetainingSeqNoException while adding retention lease (#128)
- Fixing IT Test (#137)
- Fix RetentionLeaseNotFoundException during shard reroute on follower (#138)
- Fix Flaky integ test (#140)
- Bugfix: Handled rename change for useroles in tests
- Remove the flaky assert in StartReplicationIT (#144)
- Blocked k-NN index for replication (#158)
- Gracefully fail replication on bootstrap failure. (#166)


###Infrastructure
- Add OpenSearch build script (#119)
- Add basic github workflow for build and test (#123)
- Modified github workflow to pick the latest changes (#173)



###Documentation
- Changed the arch diagram.
- Add OpenSearch Copyright
- Update documentation for OpenSearch
- Added cross cluster replication maintainers
- Add copyright to readme; Update notice (#106)
- Add documentation and templates