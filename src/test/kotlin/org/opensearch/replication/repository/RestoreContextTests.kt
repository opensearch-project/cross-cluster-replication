package org.opensearch.replication.repository

import org.apache.lucene.index.IndexCommit
import org.apache.lucene.store.IOContext
import org.apache.lucene.store.IndexInput
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.eq
import org.mockito.Mockito.*
import org.opensearch.common.concurrent.GatedCloseable
import org.opensearch.index.shard.IndexShard
import org.opensearch.index.store.Store
import org.opensearch.test.OpenSearchTestCase
import java.util.UUID

class RestoreContextTests : OpenSearchTestCase() {

    fun `test openInput returns cloned IndexInput from cache`() {
        // given
        val fileName = "test_file"

        val mockShard = mock(IndexShard::class.java)
        val mockIndexCommit = mock(GatedCloseable::class.java) as GatedCloseable<IndexCommit>
        val mockStore = mock(Store::class.java)
        val mockDirectory = mock(org.apache.lucene.store.Directory::class.java)
        val mockBaseInput = mock(IndexInput::class.java)
        val mockClonedInput = mock(IndexInput::class.java)

        // when
        `when`(mockStore.directory()).thenReturn(mockDirectory)
        `when`(mockDirectory.openInput(eq(fileName), any(IOContext::class.java))).thenReturn(mockBaseInput)
        `when`(mockBaseInput.clone()).thenReturn(mockClonedInput)
        val sut = RestoreContext(
            restoreUUID = UUID.randomUUID().toString(),
            shard = mockShard,
            indexCommitRef = mockIndexCommit,
            metadataSnapshot = Store.MetadataSnapshot.EMPTY,
            replayOperationsFrom = 0L,
        )

        val result1 = sut.openInput(mockStore, fileName)
        val result2 = sut.openInput(mockStore, fileName)

        // then
        assertSame(mockClonedInput, result1)
        assertSame(mockClonedInput, result2)
        verify(mockDirectory, times(1)).openInput(eq(fileName), any(IOContext::class.java))
        verify(mockBaseInput, times(2)).clone()
    }
}
