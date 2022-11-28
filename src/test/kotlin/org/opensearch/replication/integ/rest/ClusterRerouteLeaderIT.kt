package org.opensearch.replication.integ.rest

import org.opensearch.replication.MultiClusterRestTestCase
import org.opensearch.replication.MultiClusterAnnotations
import org.opensearch.replication.StartReplicationRequest
import org.opensearch.replication.startReplication
import org.opensearch.replication.stopReplication
import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.assertj.core.api.Assertions
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.client.indices.GetIndexRequest
import org.opensearch.test.OpenSearchTestCase.assertBusy
import org.junit.Assert
import org.junit.Assume
import org.junit.Before
import org.junit.Ignore
import java.util.concurrent.TimeUnit

@MultiClusterAnnotations.ClusterConfigurations(
        MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
        MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)

class ClusterRerouteLeaderIT : MultiClusterRestTestCase() {
    private val leaderIndexName = "leader_index"
    private val followerIndexName = "follower_index"

    @Before
    fun beforeTest() {
        Assume.assumeTrue(isMultiNodeClusterConfiguration)
    }

    fun `test replication works after rerouting a shard from one node to another in leader cluster`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        changeTemplate(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))
        insertDocToIndex(LEADER, "1", "dummy data 1",leaderIndexName)
        //Querying ES cluster throws random exceptions like ClusterManagerNotDiscovered or ShardsFailed etc, so catching them and retrying
        assertBusy ({
            try {
                Assertions.assertThat(docs(FOLLOWER, followerIndexName)).contains("dummy data 1")
            } catch (ex: Exception) {
                Assert.fail("Exception while querying follower cluster. Failing to retry again")
            }
        }, 1, TimeUnit.MINUTES)
        val nodes = getNodesInCluster(LEADER)
        val primaryNode = getPrimaryNodeForShard(LEADER,leaderIndexName, "0")
        val unassignedNode = nodes.filter{!it.equals(primaryNode)}.stream().findFirst().get()
        rerouteShard(LEADER, "0", leaderIndexName, primaryNode, unassignedNode)
        assertBusy ({
            Assertions.assertThat(getPrimaryNodeForShard(LEADER,leaderIndexName, "0")).isEqualTo(unassignedNode)
        }, 1, TimeUnit.MINUTES)
        insertDocToIndex(LEADER, "2", "dummy data 2",leaderIndexName)
        assertBusy ({
            try {
                Assertions.assertThat(docs(FOLLOWER, followerIndexName)).contains("dummy data 2")
            } catch (ex: Exception) {
                Assert.fail("Exception while querying follower cluster. Failing to retry again")
            }
        }, 1, TimeUnit.MINUTES)
    }
}
