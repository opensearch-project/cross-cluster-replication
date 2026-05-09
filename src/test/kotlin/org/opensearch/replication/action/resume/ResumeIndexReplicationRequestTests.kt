/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.replication.action.resume

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.test.OpenSearchTestCase

class ResumeIndexReplicationRequestTests : OpenSearchTestCase() {

    fun `test serialization roundtrip with forceResume false`() {
        val original = ResumeIndexReplicationRequest("my-index", forceResume = false)
        val out = BytesStreamOutput()
        original.writeTo(out)

        val inp: StreamInput = out.bytes().streamInput()
        val deserialized = ResumeIndexReplicationRequest(inp)

        assertEquals(original.indexName, deserialized.indexName)
        assertEquals(original.forceResume, deserialized.forceResume)
    }

    fun `test serialization roundtrip with forceResume true`() {
        val original = ResumeIndexReplicationRequest("my-index", forceResume = true)
        val out = BytesStreamOutput()
        original.writeTo(out)

        val inp: StreamInput = out.bytes().streamInput()
        val deserialized = ResumeIndexReplicationRequest(inp)

        assertEquals("my-index", deserialized.indexName)
        assertTrue(deserialized.forceResume)
    }

    fun `test indices returns index name`() {
        val request = ResumeIndexReplicationRequest("follower-idx")
        val indices = request.indices()
        assertEquals(1, indices.size)
        assertEquals("follower-idx", indices[0])
    }

    fun `test validate returns null`() {
        val request = ResumeIndexReplicationRequest("test-index")
        assertNull(request.validate())
    }

    fun `test fromXContent with force_resume true`() {
        val json = """{"force_resume": true}"""
        val parser = createParser(XContentType.JSON.xContent(), json)
        val request = ResumeIndexReplicationRequest.fromXContent(parser, "follower-index")
        assertEquals("follower-index", request.indexName)
        assertTrue(request.forceResume)
    }

    fun `test fromXContent with force_resume false`() {
        val json = """{"force_resume": false}"""
        val parser = createParser(XContentType.JSON.xContent(), json)
        val request = ResumeIndexReplicationRequest.fromXContent(parser, "follower-index")
        assertFalse(request.forceResume)
    }

    fun `test fromXContent with empty body defaults forceResume to false`() {
        val json = """{}"""
        val parser = createParser(XContentType.JSON.xContent(), json)
        val request = ResumeIndexReplicationRequest.fromXContent(parser, "follower-index")
        assertEquals("follower-index", request.indexName)
        assertFalse(request.forceResume)
    }

    fun `test toXContent includes force_resume field`() {
        val request = ResumeIndexReplicationRequest("test-idx", forceResume = true)
        val builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()
        request.toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS)
        val xContentString = builder.toString()
        assertTrue(xContentString.contains("\"force_resume\":true"))
        assertTrue(xContentString.contains("\"indexName\":\"test-idx\""))
    }

    fun `test indicesOptions is strict single index`() {
        val request = ResumeIndexReplicationRequest("test-index")
        val options = request.indicesOptions()
        assertFalse(options.allowNoIndices())
    }
}
