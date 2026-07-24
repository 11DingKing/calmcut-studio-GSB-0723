package com.calmcut.studio.worker

import com.calmcut.studio.domain.EventType
import com.calmcut.studio.domain.StoryboardEvent
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Verifies out-of-order buffering, in-order release, and stale-version drop. */
class VersionBufferTest {

    private fun ev(v: Long) = StoryboardEvent("e$v", "sb", v, EventType.SEGMENT_CREATED)

    @Test
    fun `in order events release immediately`() {
        val buf = VersionBuffer(0)
        assertEquals(listOf(1L), (buf.offer(ev(1)) as VersionBuffer.BufferResult.Ready).events.map { it.version })
        assertEquals(listOf(2L), (buf.offer(ev(2)) as VersionBuffer.BufferResult.Ready).events.map { it.version })
    }

    @Test
    fun `out of order events buffer until gap fills`() {
        val buf = VersionBuffer(0)
        assertTrue(buf.offer(ev(3)) is VersionBuffer.BufferResult.Buffered)
        assertTrue(buf.offer(ev(2)) is VersionBuffer.BufferResult.Buffered)
        // Arrival of v1 releases the contiguous run 1,2,3.
        val released = buf.offer(ev(1)) as VersionBuffer.BufferResult.Ready
        assertEquals(listOf(1L, 2L, 3L), released.events.map { it.version })
    }

    @Test
    fun `stale versions are dropped`() {
        val buf = VersionBuffer(5)
        assertTrue(buf.offer(ev(3)) is VersionBuffer.BufferResult.Stale)
        assertTrue(buf.offer(ev(5)) is VersionBuffer.BufferResult.Stale)
        assertTrue(buf.offer(ev(6)) is VersionBuffer.BufferResult.Ready)
    }

    @Test
    fun `duplicate buffered version is reported buffered`() {
        val buf = VersionBuffer(0)
        assertTrue(buf.offer(ev(3)) is VersionBuffer.BufferResult.Buffered)
        assertTrue(buf.offer(ev(3)) is VersionBuffer.BufferResult.Buffered)
        assertEquals(setOf(3L), buf.bufferedVersions)
    }

    @Test
    fun `advanceTo discards buffered stale versions`() {
        val buf = VersionBuffer(0)
        buf.offer(ev(2))
        buf.offer(ev(3))
        buf.advanceTo(3)
        assertEquals(4L, buf.expectedNext)
        assertTrue(buf.bufferedVersions.isEmpty())
    }
}
