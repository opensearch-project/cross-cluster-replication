package org.opensearch.replication.action.replicationstatedetails

import org.assertj.core.api.Assertions
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.test.OpenSearchTestCase
import kotlin.test.Test

class UpdateReplicationStateDetailsRequestTests : OpenSearchTestCase() {
    companion object {
        const val INDEX = "index"
    }

    @Test
    fun `test serialization update type add`() {
        val state = mapOf(Pair("k1", "v1"), Pair("k2", "v2"))
        val request = UpdateReplicationStateDetailsRequest(INDEX, state, UpdateReplicationStateDetailsRequest.UpdateType.ADD)
        val output = BytesStreamOutput()
        request.writeTo(output)
        val deserialized = UpdateReplicationStateDetailsRequest(output.bytes().streamInput())

        assertEquals(UpdateReplicationStateDetailsRequest.UpdateType.ADD, deserialized.updateType)
        assertEquals(INDEX, deserialized.followIndexName)
        Assertions.assertThat(deserialized.replicationStateParams.containsKey("k1"))
        Assertions.assertThat(deserialized.replicationStateParams.containsKey("k2"))
        Assertions.assertThat("v1".equals(deserialized.replicationStateParams["k1"]))
        Assertions.assertThat("v2".equals(deserialized.replicationStateParams["k2"]))
    }

    @Test
    fun `test serialization update type delete`() {
        val state = mapOf(Pair("k1", "v1"), Pair("k2", "v2"))
        val request = UpdateReplicationStateDetailsRequest(INDEX, state, UpdateReplicationStateDetailsRequest.UpdateType.REMOVE)
        val output = BytesStreamOutput()
        request.writeTo(output)

        val deserialized = UpdateReplicationStateDetailsRequest(output.bytes().streamInput())

        assertEquals(UpdateReplicationStateDetailsRequest.UpdateType.REMOVE, deserialized.updateType)
        assertEquals(INDEX, deserialized.followIndexName)
        Assertions.assertThat(deserialized.replicationStateParams.containsKey("k1"))
        Assertions.assertThat(deserialized.replicationStateParams.containsKey("k2"))
        Assertions.assertThat("v1".equals(deserialized.replicationStateParams["k1"]))
        Assertions.assertThat("v2".equals(deserialized.replicationStateParams["k2"]))
    }
}
