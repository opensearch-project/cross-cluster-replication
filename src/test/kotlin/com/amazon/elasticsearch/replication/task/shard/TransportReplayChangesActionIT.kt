package org.elasticsearch.replication.task.shard

import org.assertj.core.api.Assertions
import org.junit.Assert
import org.elasticsearch.action.DocWriteResponse
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.core.CountRequest
import org.elasticsearch.client.indices.PutMappingRequest
import org.elasticsearch.common.xcontent.XContentType
import com.amazon.elasticsearch.replication.*
import java.util.Locale
import java.util.concurrent.TimeUnit

const val LEADER = "leaderCluster"
const val FOLL = "followCluster"

@MultiClusterAnnotations.ClusterConfigurations(
    MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
    MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLL)
)
class TransportReplayChangesActionIT  : MultiClusterRestTestCase() {
    fun `test strict dynamic mapping update`() {
        val follower = getClientForCluster(FOLL)
        val leader = getClientForCluster(LEADER)
        createConnectionBetweenClusters(FOLL, LEADER)
        // Create a leader/follower index
        val leaderIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT)
        val followerIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT)

        try {
            val doc1 = mapOf("name" to randomAlphaOfLength(20))
            // Create Leader Index
            val response = leader.index(IndexRequest(leaderIndex).id("1").source(doc1), RequestOptions.DEFAULT)
            Assertions.assertThat(response.result)
                .withFailMessage("Failed to create leader data").isEqualTo(DocWriteResponse.Result.CREATED)

            // Setup Mapping on leader
            var putMappingRequest = PutMappingRequest(leaderIndex)
            putMappingRequest.source(
                "{\"dynamic\":\"strict\",\"properties\":{\"name\":{\"type\":\"text\"}}}",
                XContentType.JSON
            )
            leader.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT)

            // Start replication
            follower.startReplication(
                StartReplicationRequest("source", leaderIndex, followerIndex),
                waitForRestore = true
            )
            assertBusy {
                val getResponse = follower.get(GetRequest(followerIndex, "1"), RequestOptions.DEFAULT)
                Assertions.assertThat(getResponse.isExists).isTrue()
                Assertions.assertThat(getResponse.sourceAsMap).isEqualTo(doc1)
            }

            // Add a new field in mapping.
            putMappingRequest = PutMappingRequest(leaderIndex)
            putMappingRequest.source(
                "{\"dynamic\":\"strict\",\"properties\":{\"name\":{\"type\":\"text\"},\"place\":{\"type\":\"text\"}}}",
                XContentType.JSON
            )
            leader.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT)

            // Ingest a doc on the leader
            val doc2 = mapOf("name" to randomAlphaOfLength(5), "place" to randomAlphaOfLength(5))
            leader.index(IndexRequest(leaderIndex).id("2").source(doc2), RequestOptions.DEFAULT)

            // Verify that replication is working as expected.
            assertBusy ({
                Assert.assertEquals(leader.count(CountRequest(leaderIndex), RequestOptions.DEFAULT).toString(),
                    follower.count(CountRequest(followerIndex), RequestOptions.DEFAULT).toString())
                `validate status syncing response`(follower.replicationStatus(followerIndex))
                val getResponse = follower.get(GetRequest(followerIndex, "2"), RequestOptions.DEFAULT)
                Assertions.assertThat(getResponse.isExists).isTrue()
                Assertions.assertThat(getResponse.sourceAsMap).isEqualTo(doc2)
            },
                30, TimeUnit.SECONDS
            )

        } finally {
            follower.stopReplication(followerIndex)
        }

    }
}