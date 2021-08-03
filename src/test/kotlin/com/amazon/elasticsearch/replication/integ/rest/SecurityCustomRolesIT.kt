package com.amazon.elasticsearch.replication.integ.rest

import com.amazon.elasticsearch.replication.MultiClusterAnnotations
import com.amazon.elasticsearch.replication.StartReplicationRequest
import com.amazon.elasticsearch.replication.startReplication
import com.amazon.elasticsearch.replication.stopReplication
import com.amazon.elasticsearch.replication.AssumeRoles
import org.apache.http.message.BasicHeader
import org.assertj.core.api.Assertions
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.ResponseException
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.test.ESTestCase
import java.nio.charset.StandardCharsets
import java.util.Base64

@MultiClusterAnnotations.ClusterConfigurations(
        MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
        MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)
class SecurityCustomRolesIT: SecurityBase()  {
    private val leaderIndexName = "leader_index"

    fun `test that start replication works with valid user and right permissions`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "FollowerIndex1"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                    assumeRoles = AssumeRoles(remoteClusterRole = "Role1",localClusterRole = "Role1"))
            var requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
            var basicAuthHeader = BasicHeader("Authorization",
                    "Basic " + Base64.getEncoder().encodeToString("TestUser1:password".toByteArray(StandardCharsets.UTF_8)))
            requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)

            followerClient.startReplication(startReplicationRequest, requestOptions= requestOptionsBuilder.build())
            ESTestCase.assertBusy {
                Assertions.assertThat(followerClient.indices().exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT)).isEqualTo(true)
            }
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test that start replication fails with user without correct permissions`() {
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "FollowerIndex1"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()

        var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                assumeRoles = AssumeRoles(remoteClusterRole = "Role1",localClusterRole = "Role1"))
        var requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
        var basicAuthHeader = BasicHeader("Authorization",
                    "Basic " + Base64.getEncoder().encodeToString("TestUser2:password".toByteArray(StandardCharsets.UTF_8)))
        requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)

        Assertions.assertThatThrownBy { followerClient.startReplication(startReplicationRequest, requestOptions= requestOptionsBuilder.build()) }
                    .isInstanceOf(ResponseException::class.java)
                    .hasMessageContaining("403 Forbidden")
    }

    fun `test stop replication is successful with valid user`() {
        val followerClient = getClientForCluster(FOLLOWER)
        var requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
        var basicAuthHeader = BasicHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString("TestUser1:password".toByteArray(StandardCharsets.UTF_8)))
        requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)
        Assertions.assertThatThrownBy {
            followerClient.stopReplication("no_index")
        }.isInstanceOf(ResponseException::class.java)
                .hasMessageContaining("No replication in progress for index:no_index")
    }

    fun `test stop replication is NOT successful with user having invalid permissions`() {
        val followerClient = getClientForCluster(FOLLOWER)
        var requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
        var basicAuthHeader = BasicHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString("TestUser2:password".toByteArray(StandardCharsets.UTF_8)))
        requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)
        Assertions.assertThatThrownBy {
            followerClient.stopReplication("no_index", requestOptions = requestOptionsBuilder.build())
        }.isInstanceOf(ResponseException::class.java)
        .hasMessageContaining("403 Forbidden")
    }
}