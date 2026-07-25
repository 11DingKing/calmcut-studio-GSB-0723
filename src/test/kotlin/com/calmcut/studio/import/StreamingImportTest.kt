package com.calmcut.studio.import

import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Verifies the bounded-memory chunking that underpins million-segment streaming
 * imports, including resume-from-checkpoint (skip) and cooperative cancellation.
 * The DB-backed [StreamingImportService] delegates its chunking to this helper.
 */
class StreamingImportTest {

    @Test
    fun `chunks a large stream in bounded batches without materialising it`() = runTest {
        val total = 1_000_000
        var maxBatch = 0
        var count = 0L
        // A lazy flow so nothing is held in memory except one chunk at a time.
        val src = flow { repeat(total) { emit(it) } }
        src.collectChunked(size = 1000) { batch, consumed ->
            maxBatch = maxOf(maxBatch, batch.size)
            count = consumed
            true
        }
        assertEquals(total.toLong(), count)
        assertEquals(1000, maxBatch, "no batch should exceed the chunk size")
    }

    @Test
    fun `resume skips already processed items`() = runTest {
        val seen = mutableListOf<Int>()
        val src = flow { repeat(10) { emit(it) } }
        // Resume after 4 items already applied.
        src.collectChunked(size = 3, skip = 4) { batch, _ ->
            seen += batch
            true
        }
        assertEquals((4..9).toList(), seen)
    }

    @Test
    fun `cancellation stops collecting early`() = runTest {
        var processed = 0L
        val src = flow { repeat(100) { emit(it) } }
        src.collectChunked(size = 10) { _, consumed ->
            processed = consumed
            consumed < 30 // request stop once 30 consumed
        }
        // Stops right after the batch that crosses 30; no full drain.
        assertTrue(processed in 30..40, "should stop early, was $processed")
    }

    @Test
    fun `trailing partial chunk is delivered`() = runTest {
        var last = 0L
        val src = flow { repeat(25) { emit(it) } }
        src.collectChunked(size = 10) { _, consumed ->
            last = consumed
            true
        }
        assertEquals(25L, last)
    }
}
