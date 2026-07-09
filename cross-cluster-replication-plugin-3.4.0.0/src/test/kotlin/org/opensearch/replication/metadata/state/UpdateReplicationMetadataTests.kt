package org.opensearch.replication.metadata.state

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import org.junit.Assert
import org.opensearch.cluster.ClusterState
import org.opensearch.replication.action.replicationstatedetails.UpdateReplicationStateDetailsRequest
import org.opensearch.replication.metadata.ReplicationOverallState
import org.opensearch.replication.metadata.UpdateReplicationStateDetailsTaskExecutor
import org.opensearch.test.ClusterServiceUtils
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.TestThreadPool

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class UpdateReplicationMetadataTests : OpenSearchTestCase() {

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