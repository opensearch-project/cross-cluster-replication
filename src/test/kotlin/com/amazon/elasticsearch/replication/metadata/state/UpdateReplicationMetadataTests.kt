package com.amazon.elasticsearch.replication.metadata.state

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import org.junit.Assert
import org.elasticsearch.cluster.ClusterState
import com.amazon.elasticsearch.replication.action.replicationstatedetails.UpdateReplicationStateDetailsRequest
import com.amazon.elasticsearch.replication.metadata.ReplicationOverallState
import com.amazon.elasticsearch.replication.metadata.UpdateReplicationStateDetailsTaskExecutor
import org.elasticsearch.test.ClusterServiceUtils
import org.elasticsearch.test.ESTestCase
import org.elasticsearch.threadpool.TestThreadPool

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class UpdateReplicationMetadataTests : ESTestCase() {

    var threadPool = TestThreadPool("ReplicationPluginTest")
    var clusterService  = ClusterServiceUtils.createClusterService(threadPool)

    fun `test single task update`() {
        val currentState: ClusterState = clusterService.state()
        // single task
        val tasks = arrayListOf(UpdateReplicationStateDetailsRequest("test-index",
                hashMapOf("REPLICATION_LAST_KNOWN_OVERALL_STATE" to "RUNNING"), UpdateReplicationStateDetailsRequest.UpdateType.ADD))
        val tasksResult = UpdateReplicationStateDetailsTaskExecutor.INSTANCE.execute(currentState, tasks)

        val updatedReplicationDetails = tasksResult.resultingState?.metadata
                ?.custom<ReplicationStateMetadata>(ReplicationStateMetadata.NAME)?.replicationDetails

        Assert.assertNotNull(updatedReplicationDetails)
        Assert.assertNotNull(updatedReplicationDetails?.get("test-index"))
        val replicationStateParams = updatedReplicationDetails?.get("test-index")

        Assert.assertEquals(ReplicationOverallState.RUNNING.name, replicationStateParams?.get(REPLICATION_LAST_KNOWN_OVERALL_STATE))
    }

    fun `test multiple tasks to add replication metadata`() {
        val currentState: ClusterState = clusterService.state()
        // multiple tasks
        val tasks = arrayListOf(UpdateReplicationStateDetailsRequest("test-index-1",
                hashMapOf("REPLICATION_LAST_KNOWN_OVERALL_STATE" to "RUNNING"), UpdateReplicationStateDetailsRequest.UpdateType.ADD),
                UpdateReplicationStateDetailsRequest("test-index-2",
                        hashMapOf("REPLICATION_LAST_KNOWN_OVERALL_STATE" to "RUNNING"), UpdateReplicationStateDetailsRequest.UpdateType.ADD))
        val tasksResult = UpdateReplicationStateDetailsTaskExecutor.INSTANCE.execute(currentState, tasks)

        val updatedReplicationDetails = tasksResult.resultingState?.metadata
                ?.custom<ReplicationStateMetadata>(ReplicationStateMetadata.NAME)?.replicationDetails

        Assert.assertNotNull(updatedReplicationDetails)
        Assert.assertNotNull(updatedReplicationDetails?.get("test-index-1"))
        var replicationStateParams = updatedReplicationDetails?.get("test-index-1")
        Assert.assertEquals(ReplicationOverallState.RUNNING.name, replicationStateParams?.get(REPLICATION_LAST_KNOWN_OVERALL_STATE))
        Assert.assertNotNull(updatedReplicationDetails?.get("test-index-2"))
        replicationStateParams = updatedReplicationDetails?.get("test-index-2")
        Assert.assertEquals(ReplicationOverallState.RUNNING.name, replicationStateParams?.get(REPLICATION_LAST_KNOWN_OVERALL_STATE))
    }

    fun `test multiple tasks to add and delete replication metadata`() {
        val currentState: ClusterState = clusterService.state()
        // multiple tasks
        val tasks = arrayListOf(UpdateReplicationStateDetailsRequest("test-index-1",
                hashMapOf("REPLICATION_LAST_KNOWN_OVERALL_STATE" to "RUNNING"), UpdateReplicationStateDetailsRequest.UpdateType.ADD),
                UpdateReplicationStateDetailsRequest("test-index-2",
                        hashMapOf("REPLICATION_LAST_KNOWN_OVERALL_STATE" to "RUNNING"), UpdateReplicationStateDetailsRequest.UpdateType.REMOVE))
        val tasksResult = UpdateReplicationStateDetailsTaskExecutor.INSTANCE.execute(currentState, tasks)

        val updatedReplicationDetails = tasksResult.resultingState?.metadata
                ?.custom<ReplicationStateMetadata>(ReplicationStateMetadata.NAME)?.replicationDetails

        Assert.assertNotNull(updatedReplicationDetails)
        Assert.assertNotNull(updatedReplicationDetails?.get("test-index-1"))
        var replicationStateParams = updatedReplicationDetails?.get("test-index-1")
        Assert.assertEquals(ReplicationOverallState.RUNNING.name, replicationStateParams?.get(REPLICATION_LAST_KNOWN_OVERALL_STATE))
        Assert.assertNull(updatedReplicationDetails?.get("test-index-2"))
    }
}
