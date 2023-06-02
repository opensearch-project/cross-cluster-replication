package com.amazon.elasticsearch.replication.integ.rest

import com.amazon.elasticsearch.replication.MultiClusterRestTestCase
import com.amazon.elasticsearch.replication.MultiClusterAnnotations
import com.amazon.elasticsearch.replication.StartReplicationRequest
import com.amazon.elasticsearch.replication.startReplication
import com.amazon.elasticsearch.replication.stopReplication
import org.assertj.core.api.Assertions
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.indices.CreateIndexRequest
import org.junit.Assert
import java.util.concurrent.TimeUnit


@MultiClusterAnnotations.ClusterConfigurations(
        MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
        MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)

class ReplicationStopThenRestartIT : MultiClusterRestTestCase() {
    private val leaderIndexName = "leader_index"
    private val followerIndexName = "follower_index"

    fun `test replication works after unclean stop and start`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        changeTemplate(LEADER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)
        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))
        insertDocToIndex(LEADER, "1", "dummy data 1",leaderIndexName)
        insertDocToIndex(LEADER, "2", "dummy data 1",leaderIndexName)

        assertBusy ({
            try {
                Assert.assertEquals(2, docCount(followerClient, followerIndexName))
            } catch (ex: Exception) {
                Assert.fail("Exception while querying follower cluster. Failing to retry again $ex")
            }
        }, 1, TimeUnit.MINUTES)


        deleteConnection(FOLLOWER)
        followerClient.stopReplication(followerIndexName, shouldWait = true)
        deleteIndex(followerClient, followerIndexName)

        createConnectionBetweenClusters(FOLLOWER, LEADER)
        insertDocToIndex(LEADER, "3", "dummy data 1",leaderIndexName)
        insertDocToIndex(LEADER, "4", "dummy data 1",leaderIndexName)
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))

        assertBusy ({
            try {
                Assert.assertEquals(4, docCount(followerClient, followerIndexName))
            } catch (ex: Exception) {
                Assert.fail("Exception while querying follower cluster. Failing to retry again")
            }
        }, 1, TimeUnit.MINUTES)
    }
}
