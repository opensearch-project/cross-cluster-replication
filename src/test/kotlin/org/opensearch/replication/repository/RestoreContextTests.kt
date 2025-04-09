package org.opensearch.replication.repository

import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import org.apache.lucene.index.IndexCommit
import org.apache.lucene.store.Directory
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

        val mockShard = mock<IndexShard>()
        val mockIndexCommit = mock<GatedCloseable<IndexCommit>>()
        val mockDirectory = mock<Directory>()
        val mockBaseInput = mock<IndexInput>()
        val mockClonedInput = mock<IndexInput>()
        val mockStore = mock<Store> {
            on { directory() } doReturn mockDirectory
        }

        whenever(mockDirectory.openInput(eq(fileName), any())).thenReturn(mockBaseInput)
        whenever(mockBaseInput.clone()).thenReturn(mockClonedInput)

        val sut = object : RestoreContext(
            restoreUUID = UUID.randomUUID().toString(),
            shard = mockShard,
            indexCommitRef = mockIndexCommit,
            metadataSnapshot = Store.MetadataSnapshot.EMPTY,
            replayOperationsFrom = 0L,
        ) {
            override fun withStoreReference(store: Store, block: () -> Unit) {
                block()
            }
        }

        // when
        val result1 = sut.openInput(mockStore, fileName)
        val result2 = sut.openInput(mockStore, fileName)

        // then
        assertSame(mockClonedInput, result1)
        assertSame(mockClonedInput, result2)
        verify(mockDirectory, times(1)).openInput(eq(fileName), any())
        verify(mockBaseInput, times(2)).clone()
    }
}
