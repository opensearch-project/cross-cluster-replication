package com.amazon.elasticsearch.replication.integ.rest

import com.amazon.elasticsearch.replication.*
import org.apache.http.message.BasicHeader
import org.assertj.core.api.Assertions
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.ResponseException
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.common.settings.Settings
import org.junit.Assert
import org.junit.Assume
import java.nio.charset.StandardCharsets
import java.util.*

@MultiClusterAnnotations.ClusterConfigurations(
        MultiClusterAnnotations.ClusterConfiguration(clusterName = LEADER),
        MultiClusterAnnotations.ClusterConfiguration(clusterName = FOLLOWER)
)
class SecurityDlsFlsIT: SecurityBase() {
    private val leaderIndexName = "leader_index"
    private val DLS_FLS_EXCEPTION_MESSAGE = "Cross Cluster Replication is not supported when FLS or DLS or Fieldmasking is activated"

    fun `test START replication is forbidden for user with DLS or FLS enabled`() {
        Assume.assumeTrue(isSecurityEnabled)
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        val followerIndexName = "follower-index1"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()

        var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                    assumeRoles = AssumeRoles(remoteClusterRole = "role1",localClusterRole = "role3"))
        var requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
        var basicAuthHeader = BasicHeader("Authorization",
                    "Basic " + Base64.getEncoder().encodeToString("testUser3:password".toByteArray(StandardCharsets.UTF_8)))
        requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)

        Assertions.assertThatThrownBy { followerClient.startReplication(startReplicationRequest, requestOptions= requestOptionsBuilder.build()) }
                    .isInstanceOf(ResponseException::class.java)
                    .hasMessageContaining(DLS_FLS_EXCEPTION_MESSAGE)
    }

    fun `test STOP replication is forbidden for user with DLS or FLS enabled`() {
        Assume.assumeTrue(isSecurityEnabled)
        val followerClient = getClientForCluster(FOLLOWER)
        var requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
        var basicAuthHeader = BasicHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString("testUser3:password".toByteArray(StandardCharsets.UTF_8)))
        requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)
        Assertions.assertThatThrownBy {
            followerClient.stopReplication("follower-index1", requestOptions = requestOptionsBuilder.build())
        }.isInstanceOf(ResponseException::class.java)
        .hasMessageContaining(DLS_FLS_EXCEPTION_MESSAGE)
    }

    fun `test PAUSE replication is forbidden for user with DLS or FLS enabled`() {
        Assume.assumeTrue(isSecurityEnabled)
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                    assumeRoles = AssumeRoles(remoteClusterRole = "role1",localClusterRole = "role1"))
            var requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
            var basicAuthHeader = BasicHeader("Authorization",
                    "Basic " + Base64.getEncoder().encodeToString("testUser1:password".toByteArray(StandardCharsets.UTF_8)))
            requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)

            followerClient.startReplication(startReplicationRequest, requestOptions= requestOptionsBuilder.build(), waitForRestore = true)

            requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
            basicAuthHeader = BasicHeader("Authorization",
                    "Basic " + Base64.getEncoder().encodeToString("testUser3:password".toByteArray(StandardCharsets.UTF_8)))
            requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)

            Assertions.assertThatThrownBy {
                followerClient.pauseReplication(followerIndexName, requestOptions = requestOptionsBuilder.build())
            }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining(DLS_FLS_EXCEPTION_MESSAGE)
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test STATUS Api is forbidden for user with DLS or FLS enabled`() {
        Assume.assumeTrue(isSecurityEnabled)
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                    assumeRoles = AssumeRoles(remoteClusterRole = "role1",localClusterRole = "role1"))
            var requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
            var basicAuthHeader = BasicHeader("Authorization",
                    "Basic " + Base64.getEncoder().encodeToString("testUser1:password".toByteArray(StandardCharsets.UTF_8)))
            requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)

            followerClient.startReplication(startReplicationRequest, requestOptions= requestOptionsBuilder.build(), waitForRestore = true)

            requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
            basicAuthHeader = BasicHeader("Authorization",
                    "Basic " + Base64.getEncoder().encodeToString("testUser3:password".toByteArray(StandardCharsets.UTF_8)))
            requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)
            Assertions.assertThatThrownBy {
                followerClient.replicationStatus(followerIndexName,requestOptions = requestOptionsBuilder.build())
            }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining(DLS_FLS_EXCEPTION_MESSAGE)
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test UPDATE settings is forbidden for user with DLS or FLS enabled`() {
        Assume.assumeTrue(isSecurityEnabled)
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)
        val followerIndexName = "follower-index1"

        setMetadataSyncDelay()

        createConnectionBetweenClusters(FOLLOWER, LEADER)

        var settings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .build()

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName).settings(settings), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()
        try {
            var requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
            var basicAuthHeader = BasicHeader("Authorization",
                    "Basic " + Base64.getEncoder().encodeToString("testUser1:password".toByteArray(StandardCharsets.UTF_8)))
            requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)
            followerClient.startReplication(StartReplicationRequest("source", leaderIndexName, followerIndexName,
                    assumeRoles = AssumeRoles(remoteClusterRole = "role1",localClusterRole = "role1")),
                    requestOptions = requestOptionsBuilder.build())
            assertBusy {
                Assertions.assertThat(followerClient.indices()
                        .exists(GetIndexRequest(followerIndexName), RequestOptions.DEFAULT))
                        .isEqualTo(true)
            }
            val getSettingsRequest = GetSettingsRequest()
            getSettingsRequest.indices(followerIndexName)
            Assert.assertEquals(
                    "1",
                    followerClient.indices()
                            .getSettings(getSettingsRequest, RequestOptions.DEFAULT)
                            .indexToSettings[followerIndexName][IndexMetadata.SETTING_NUMBER_OF_REPLICAS]
            )

            settings = Settings.builder()
                    .put("index.shard.check_on_startup", "checksum")
                    .build()

            requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
            basicAuthHeader = BasicHeader("Authorization",
                    "Basic " + Base64.getEncoder().encodeToString("testUser3:password".toByteArray(StandardCharsets.UTF_8)))
            requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)
            Assertions.assertThatThrownBy {
                followerClient.updateReplication(followerIndexName, settings, requestOptionsBuilder.build())
            }.isInstanceOf(ResponseException::class.java)
            .hasMessageContaining(DLS_FLS_EXCEPTION_MESSAGE)
        } finally {
            followerClient.stopReplication(followerIndexName)
        }
    }

    fun `test START replication is forbidden for user with FLS enabled`() {
        Assume.assumeTrue(isSecurityEnabled)
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        val followerIndexName = "follower-index1"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()

        var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                assumeRoles = AssumeRoles(remoteClusterRole = "role1",localClusterRole = "role4"))
        var requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
        var basicAuthHeader = BasicHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString("testUser4:password".toByteArray(StandardCharsets.UTF_8)))
        requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)

        Assertions.assertThatThrownBy { followerClient.startReplication(startReplicationRequest, requestOptions= requestOptionsBuilder.build()) }
                .isInstanceOf(ResponseException::class.java)
                .hasMessageContaining(DLS_FLS_EXCEPTION_MESSAGE)
    }

    fun `test START replication is forbidden for user with Field Masking enabled`() {
        Assume.assumeTrue(isSecurityEnabled)
        val followerClient = getClientForCluster(FOLLOWER)
        val leaderClient = getClientForCluster(LEADER)

        val followerIndexName = "follower-index1"
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val createIndexResponse = leaderClient.indices().create(CreateIndexRequest(leaderIndexName), RequestOptions.DEFAULT)
        Assertions.assertThat(createIndexResponse.isAcknowledged).isTrue()

        var startReplicationRequest = StartReplicationRequest("source",leaderIndexName,followerIndexName,
                assumeRoles = AssumeRoles(remoteClusterRole = "role1",localClusterRole = "role5"))
        var requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder()
        var basicAuthHeader = BasicHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString("testUser5:password".toByteArray(StandardCharsets.UTF_8)))
        requestOptionsBuilder.addHeader(basicAuthHeader.name, basicAuthHeader.value)

        Assertions.assertThatThrownBy { followerClient.startReplication(startReplicationRequest, requestOptions= requestOptionsBuilder.build()) }
                .isInstanceOf(ResponseException::class.java)
                .hasMessageContaining(DLS_FLS_EXCEPTION_MESSAGE)
    }
}