package org.opensearch.replication.integ.rest

import org.assertj.core.api.Assertions
import org.junit.Assert
import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.replication.MultiClusterAnnotations
import org.opensearch.replication.MultiClusterRestTestCase
import org.opensearch.replication.StartReplicationRequest
import org.opensearch.replication.startReplication
import org.opensearch.replication.stopReplication
import java.util.concurrent.TimeUnit

@MultiClusterAnnotations.ClusterConfigurations(
    MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
    MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER),
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
        insertDocToIndex(LEADER, "1", "dummy data 1", leaderIndexName)
        insertDocToIndex(LEADER, "2", "dummy data 1", leaderIndexName)

        assertBusy({
            try {
                Assert.assertEquals(2, docCount(followerClient, followerIndexName))
            } catch (ex: Exception) {
                ex.printStackTrace()
                Assert.fail("Exception while querying follower cluster. Failing to retry again {}")
            }
        }, 1, TimeUnit.MINUTES)

        deleteConnection(FOLLOWER)
        followerClient.stopReplication(followerIndexName, shouldWait = true)
        deleteIndex(followerClient, followerIndexName)

        createConnectionBetweenClusters(FOLLOWER, LEADER)
        insertDocToIndex(LEADER, "3", "dummy data 1", leaderIndexName)
        insertDocToIndex(LEADER, "4", "dummy data 1", leaderIndexName)
        followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName))

        assertBusy({
            try {
                Assert.assertEquals(4, docCount(followerClient, followerIndexName))
            } catch (ex: Exception) {
                Assert.fail("Exception while querying follower cluster. Failing to retry again")
            }
        }, 1, TimeUnit.MINUTES)
    }
}
