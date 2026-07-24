package com.calmcut.studio.worker

import com.calmcut.studio.domain.model.StoryboardSegment
import com.calmcut.studio.event.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.assertIs

class VersionBufferTest {

    private lateinit var buffer: VersionBuffer

    @BeforeEach
    fun setup() {
        buffer = VersionBuffer()
    }

    private fun seg(id: String, orderIndex: Int): StoryboardSegment =
        StoryboardSegment(
            id = id, timelineId = "tl-1", version = 1,
            orderIndex = orderIndex, startTimeMs = orderIndex * 1000L,
            endTimeMs = (orderIndex + 1) * 1000L, intensity = 3,
            isReversal = false, isKnowledgePoint = false, isDeclineInducement = false
        )

    private fun event(version: Long, aggregateId: String = "seg-1"): SegmentUpdated =
        SegmentUpdated(
            eventId = UUID.randomUUID().toString(),
            timelineId = "tl-1",
            aggregateId = aggregateId,
            version = version,
            previousVersion = seg("seg-1", 0),
            segment = seg("seg-1", 0),
            expectedVersion = version - 1
        )

    @Test
    fun `events in order are immediately processed`() {
        val result1 = buffer.offer(event(1))
        assertIs<VersionBuffer.BufferResult.Ready>(result1)
        assertEquals(1, result1.events.size)
        assertEquals(1L, result1.events[0].version)

        val result2 = buffer.offer(event(2))
        assertIs<VersionBuffer.BufferResult.Ready>(result2)
        assertEquals(1, result2.events.size)
    }

    @Test
    fun `out of order event is buffered until predecessor arrives`() {
        val result3 = buffer.offer(event(3))
        assertIs<VersionBuffer.BufferResult.Buffered>(result3)
        assertEquals(1L, result3.expectedVersion)
        assertEquals(3L, result3.receivedVersion)

        val result1 = buffer.offer(event(1))
        assertIs<VersionBuffer.BufferResult.Ready>(result1)
        assertEquals(1, result1.events.size, "Should drain v1 only; v2 is still missing")
    }

    @Test
    fun `gap closes when all intermediate events arrive`() {
        buffer.offer(event(3))
        buffer.offer(event(5))

        buffer.offer(event(1))
        val result2 = buffer.offer(event(2))
        assertIs<VersionBuffer.BufferResult.Ready>(result2)
        assertEquals(2, result2.events.size, "v2 arrival should drain v2 and v3")

        val result4 = buffer.offer(event(4))
        assertIs<VersionBuffer.BufferResult.Ready>(result4)
        assertEquals(2, result4.events.size, "v4 arrival should drain v4, v5")
    }

    @Test
    fun `duplicate event (already processed version) is detected`() {
        buffer.offer(event(1))
        val duplicate = buffer.offer(event(1))
        assertIs<VersionBuffer.BufferResult.Duplicate>(duplicate)
    }

    @Test
    fun `stale event (lower version than processed) is detected`() {
        buffer.offer(event(1))
        buffer.offer(event(2))
        val stale = buffer.offer(event(1))
        assertIs<VersionBuffer.BufferResult.Duplicate>(stale)
    }

    @Test
    fun `different aggregates have independent buffers`() {
        buffer.offer(event(1, "seg-a"))
        buffer.offer(event(1, "seg-b"))

        assertEquals(1L, buffer.getProcessedVersion("seg-a"))
        assertEquals(1L, buffer.getProcessedVersion("seg-b"))
    }

    @Test
    fun `set processed version directly works for recovery`() {
        buffer.setProcessedVersion("seg-1", 5)
        val result = buffer.offer(event(6))
        assertIs<VersionBuffer.BufferResult.Ready>(result)
        assertEquals(6L, result.events[0].version)
    }

    @Test
    fun `batch imported events on timeline aggregate are buffered correctly`() {
        fun batchEvent(version: Long) = BatchImported(
            eventId = UUID.randomUUID().toString(),
            timelineId = "tl-1",
            aggregateId = "tl-1",
            version = version,
            segments = listOf(seg("seg-1", 0))
        )

        buffer.offer(batchEvent(2))
        assertIs<VersionBuffer.BufferResult.Buffered>(buffer.offer(batchEvent(3)))

        val result1 = buffer.offer(batchEvent(1))
        assertIs<VersionBuffer.BufferResult.Ready>(result1)
        assertEquals(3, result1.events.size, "v1 arrival should chain to drain v1, v2, v3")
    }

    @Test
    fun `buffer clears aggregate state`() {
        buffer.offer(event(3))
        assertEquals(1, buffer.size("seg-1"))
        buffer.clear("seg-1")
        assertEquals(0, buffer.size("seg-1"))
    }
}
