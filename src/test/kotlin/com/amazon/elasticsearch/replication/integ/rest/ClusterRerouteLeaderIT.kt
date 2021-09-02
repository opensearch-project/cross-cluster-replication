package com.amazon.elasticsearch.replication.integ.rest

import com.amazon.elasticsearch.replication.MultiClusterRestTestCase
import com.amazon.elasticsearch.replication.MultiClusterAnnotations
import com.amazon.elasticsearch.replication.StartReplicationRequest
import com.amazon.elasticsearch.replication.startReplication
import com.amazon.elasticsearch.replication.stopReplication
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.assertj.core.api.Assertions
import org.elasticsearch.client.Request
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.test.ESTestCase.assertBusy
import org.junit.Assert
import org.junit.Ignore
import java.util.concurrent.TimeUnit

@MultiClusterAnnotations.ClusterConfigurations(
        MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
        MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)

class ClusterRerouteLeaderIT : MultiClusterRestTestCase() {
    private val leaderIndexName = "leader_index"
    private val followerIndexName = "follower_index"

    fun `test replication works after rerouting a shard from one node to another in leader cluster`() {
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
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }
}
