# cross-cluster-replication plugin
Integ Tests Integ Tests with Security plugin BWC Tests codecov

Cross Cluster Replication Plugin
Build
Building from the command line
Intellij Setup
Getting Started
Step 1: Start test clusters with replication plugin locally
Step 2: Setup cross-cluster connectivity
Step 3: Populate leader cluster with sample data
Step 4: Start replication
Step 5: Make changes to data on leader index
Step 6: Validate replicated content on the follower
Step 7: Stop replication
CONTRIBUTING GUIDELINES
License
Cross-Cluster Replication Plugin enables users to replicate data across two opensearch clusters which enables a number of use cases such as

Disaster Recovery (DR) or High Availability (HA): For production systems with high availability requirements, cross-cluster replication provides the safety-net of being able to failover to an alternate cluster in case of failure or outages on the primary cluster.
Reduced Query Latency: For critical business needs, responding to the customer query in the shortest time is critical. Replicating data to a cluster that is closest to the user can drastically reduce the query latency. Applications can redirect the customer query to the nearest data center where data has been replicated.
Scaling out query heavy workloads: Splitting a query heavy workload across multiple replica clusters improves horizontal scalability.
Aggregated reports - Enterprise customers can roll up reports continually from smaller clusters belonging to different lines of business into a central cluster for consolidated reports, dashboards or visualizations.
Following are the tenets that guided our design:

Secure: Cross-cluster replication should offer strong security controls for all flows and APIs.
Correctness: There must be no difference between the intended contents of the follower index and the leader index.
Performance: Replication should not impact indexing rate of the leader cluster.
Lag: The replication lag between the leader and the follower cluster should be under a few seconds.
Resource usage: Replication should use minimal resources.
The replication machinery is implemented as an OpenSearch plugin that exposes APIs to control replication, spawns background persistent tasks to asynchronously replicate indices and utilizes snapshot repository abstraction to facilitate bootstrap. Replication relies on cross cluster connection setup from the follower cluster to the leader cluster for connectivity. Once replication is initiated on an index, a background persistent task per primary shard on the follower cluster continuously polls corresponding shards from the leader index and applies the changes on to the follower shard. The replication plugin offers seamless integration with the OpenSearch Security plugin for secure data transfer and access control.

Build
The project in this package uses the Gradle build system. Gradle comes with excellent documentation that should be your first stop when trying to figure out how to operate or modify the build.

Building from the command line
Set JAVA_HOME to JDK-21 or above

./gradlew build builds and tests project.
./gradlew clean release cleans previous builds, creates new build and tests project.
./gradlew clean run -PnumNodes=3 launches a 3 node cluster of both leader and follower with replication plugin installed.
./gradlew integTest launches a single node cluster's and runs all integ tests.
./gradlew integTest -Dtests.class=*{class-name} runs a single integ class.
./gradlew integTest -Dtests.class=*{class-name} -Dtests.method="{method-name}" runs a single integ test method (remember to quote the test method name if it contains spaces).
Intellij Setup
Launch Intellij IDEA, choose Import Project, and select the settings.gradle file in the root of this package.

Getting Started
Following steps will help you install the replication plugin on a test cluster.

Step 1: Start test clusters with replication plugin locally
./gradlew clean run -PnumNodes=3


# Set variables for readability (in different terminal window/tab where you will run rest of the steps)
export LEADER=localhost:9200
export FOLLOWER=localhost:9203
export LEADER_TRANSPORT=localhost:9300
Step 2: Setup cross-cluster connectivity
Setup remote cluster connection from follower cluster to the leader cluster

curl -XPUT "http://${FOLLOWER}/_cluster/settings?pretty" \
-H 'Content-Type: application/json' -d"
{
  \"persistent\": {
    \"cluster\": {
      \"remote\": {
        \"leader-cluster\": {
          \"seeds\": [ \"${LEADER_TRANSPORT}\" ]
        }
      }
    }
  }
}
"
Step 3: Populate leader cluster with sample data
curl -XPOST "http://${LEADER}/leader-01/_doc/1" -H 'Content-Type: application/json' -d '{"value" : "data1"}'
Step 4: Start replication
curl -XPUT "http://${FOLLOWER}/_plugins/_replication/follower-01/_start?pretty" \
-H 'Content-type: application/json' \
-d'{"leader_alias":"leader-cluster", "leader_index": "leader-01"}'
Step 5: Make changes to data on leader index
# 1. Modify doc with id 1
curl -XPOST "http://${LEADER}/leader-01/_doc/1" \
-H 'Content-Type: application/json' -d '{"value" : "data1-modified"}'

# 2. Add doc with id 2
curl -XPOST "http://${LEADER}/leader-01/_doc/2" \
-H 'Content-Type: application/json' -d '{"value" : "data2"}'
Step 6: Validate replicated content on the follower
# 1. Validate replicated index exists
curl -XGET "http://${FOLLOWER}/_cat/indices"
# The above should list "follower-01" as on of the index as well

# 2. Check content of follower-01
curl -XGET "http://${FOLLOWER}/follower-01/_search"
# The above should list 2 documents with id 1 and 2 and matching content of
# leader-01 index on $LEADER cluster
At this point, any changes to leader-01 continues to be replicated to follower-01.

Step 7: Stop replication
Stopping replication opens up the replicated index on the follower cluster for writes. This can be leveraged to failover to the follower cluster when the need arises.

curl -XPOST "http://${FOLLOWER}/_plugins/_replication/follower-01/_stop?pretty" \
-H 'Content-type: application/json' -d'{}'

# You can confirm data isn't replicated any more by making modifications to
# leader-01 index on $LEADER cluster 
For much detailed instructions/examples please refer to HANDBOOK.

For more details on design and architecture, please refer to RFC under docs.

CONTRIBUTING GUIDELINES
See CONTRIBUTING for more information.

Getting Help
If you find a bug, or have a feature request, please don't hesitate to open an issue in this repository.

For more information, see the project website and technical documentation. If you need help and are unsure where to open an issue, try the OpenSearch Forum.

License
This project is licensed under the Apache-2.0 License.

Copyright
Copyright OpenSearch Contributors. See NOTICE for details.
