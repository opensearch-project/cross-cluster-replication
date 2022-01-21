package org.opensearch.replication.integ.rest

import org.opensearch.replication.MultiClusterRestTestCase
import org.opensearch.replication.MultiClusterAnnotations
import org.opensearch.replication.StartReplicationRequest
import org.opensearch.replication.startReplication
import org.opensearch.replication.stopReplication
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.assertj.core.api.Assertions
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.client.indices.GetIndexRequest
import org.opensearch.test.OpenSearchTestCase.assertBusy
import org.junit.Assert
import org.junit.Assume
import org.junit.Before
import java.util.concurrent.TimeUnit


@MultiClusterAnnotations.ClusterConfigurations(
        MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
        MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)

class ClusterRerouteFollowerIT : MultiClusterRestTestCase() {
    private val leaderIndexName = "leader_index"
    private val followerIndexName = "follower_index"

    @Before
    fun beforeTest() {
        Assume.assumeTrue(isMultiNodeClusterConfiguration)
    }

    fun `test replication works after rerouting a shard from one node to another in follower cluster`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        try {
            changeTemplate(LEADER)
            createConnectionBetweenClusters(FOLLOWER, LEADER)
            val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
            Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))
            insertDocToIndex(LEADER, "1", "dummy data 1",leaderIndexName)

            //Querying ES cluster throws random exceptions like MasterNotDiscovered or ShardsFailed etc, so catching them and retrying
            assertBusy ({
                try {
                    Assertions.assertThat(docs(FOLLOWER, followerIndexName)).contains("dummy data 1")
                } catch (ex: Exception) {
                    Assert.fail("Exception while querying follower cluster. Failing to retry again")
                }
            }, 1, TimeUnit.MINUTES)

            val nodes = getNodesInCluster(FOLLOWER)

            val primaryNode = getPrimaryNodeForShard(FOLLOWER,followerIndexName, "0")
            val unassignedNode = nodes.filter{!it.equals(primaryNode)}.stream().findFirst().get()
            rerouteShard(FOLLOWER, "0", followerIndexName, primaryNode, unassignedNode)

            assertBusy ({
                Assertions.assertThat(getPrimaryNodeForShard(FOLLOWER,followerIndexName, "0")).isEqualTo(unassignedNode)
            }, 1, TimeUnit.MINUTES)
            logger.info("rereouted shard is " + getPrimaryNodeForShard(FOLLOWER,followerIndexName, "0"))
            insertDocToIndex(LEADER, "2", "dummy data 2",leaderIndexName)

            assertBusy ({
                try {
                    Assertions.assertThat(docs(FOLLOWER, followerIndexName)).contains("dummy data 2")
                } catch (ex: Exception) {
                    Assert.fail("Exception while querying follower cluster. Failing to retry again")
                }
            }, 1, TimeUnit.MINUTES)
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }
}
