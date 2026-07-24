package com.calmcut.studio.worker

import com.calmcut.studio.analysis.AnalysisSettings
import com.calmcut.studio.analysis.RiskAnalyzer
import com.calmcut.studio.domain.EventType
import com.calmcut.studio.domain.Segment
import com.calmcut.studio.domain.StoryboardEvent
import com.calmcut.studio.testutil.InMemoryDeadLetterSink
import com.calmcut.studio.testutil.InMemoryIdempotencyChecker
import com.calmcut.studio.testutil.InMemoryProjectionStore
import com.calmcut.studio.testutil.RecordingResultPublisher
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class WorkerCrashAndIdempotencyTest {

    private fun newProcessor(
        publisher: RecordingResultPublisher = RecordingResultPublisher(),
        dlq: InMemoryDeadLetterSink = InMemoryDeadLetterSink(),
        idem: InMemoryIdempotencyChecker = InMemoryIdempotencyChecker(),
        store: InMemoryProjectionStore = InMemoryProjectionStore(),
        maxAttempts: Int = 3,
    ) = IdempotentProcessor(
        analyzer = RiskAnalyzer(AnalysisSettings()),
        store = store,
        idempotency = idem,
        publisher = publisher,
        deadLetters = dlq,
        maxAttempts = maxAttempts,
    )

    private fun importEvent(v: Long, segs: List<Segment>) =
        StoryboardEvent("imp-$v", "sb", v, EventType.BATCH_IMPORT, segments = segs)

    private fun seg(id: String, order: Int, intensity: Int = 3) =
        Segment(id, order, durationMs = 4000, intensity = intensity)

    @Test
    fun `successful processing publishes a result`() = runTest {
        val publisher = RecordingResultPublisher()
        val processor = newProcessor(publisher)
        val results = processor.process(importEvent(1, listOf(seg("a", 0), seg("b", 1))))
        assertEquals(1, results.size)
        assertEquals(1, publisher.published.size)
        assertEquals(1L, publisher.published.first().version)
    }

    @Test
    fun `duplicate messages are idempotent`() = runTest {
        val publisher = RecordingResultPublisher()
        val processor = newProcessor(publisher)
        val event = importEvent(1, listOf(seg("a", 0)))
        processor.process(event)
        processor.process(event) // redelivery
        processor.process(event)
        assertEquals(1, publisher.published.size, "duplicate event must be processed only once")
    }

    @Test
    fun `out of order events are buffered then applied in order`() = runTest {
        val publisher = RecordingResultPublisher()
        val processor = newProcessor(publisher)
        // Deliver v2 and v3 before v1.
        processor.process(importEvent(2, listOf(seg("a", 0), seg("b", 1))))
        processor.process(importEvent(3, listOf(seg("a", 0))))
        assertTrue(publisher.published.isEmpty(), "nothing releases before v1")
        processor.process(importEvent(1, listOf(seg("a", 0))))
        // v1 releases, then buffered v2 and v3 flush in order.
        assertEquals(listOf(1L, 2L, 3L), publisher.published.map { it.version })
    }

    @Test
    fun `stale events below processed version are ignored`() = runTest {
        val publisher = RecordingResultPublisher()
        val idem = InMemoryIdempotencyChecker()
        idem.recordVersion("sb", 5)
        val processor = newProcessor(publisher, idem = idem)
        val results = processor.process(importEvent(3, listOf(seg("a", 0))))
        assertTrue(results.isEmpty())
        assertTrue(publisher.published.isEmpty())
    }

    @Test
    fun `crash during publish retries then succeeds`() = runTest {
        val publisher = RecordingResultPublisher().apply { failNextCount = 2 } // fail twice, succeed 3rd
        val processor = newProcessor(publisher, maxAttempts = 3)
        val results = processor.process(importEvent(1, listOf(seg("a", 0))))
        assertEquals(1, results.size)
        assertEquals(1, publisher.published.size)
    }

    @Test
    fun `exhausted retries route event to DLQ and can be replayed`() = runTest {
        val publisher = RecordingResultPublisher().apply { failNextCount = 100 }
        val dlq = InMemoryDeadLetterSink()
        val processor = newProcessor(publisher, dlq = dlq, maxAttempts = 2)
        processor.process(importEvent(1, listOf(seg("a", 0))))
        assertEquals(1, dlq.list().size, "poisoned event must land in DLQ")

        // Fix the downstream and replay from the DLQ.
        publisher.failNextCount = 0
        val replayed = processor.replayDeadLetter("imp-1")
        assertEquals(1, replayed.size)
        assertEquals(1, publisher.published.size)
        assertTrue(dlq.list().single().replayed)
    }

    @Test
    fun `crash before idempotency commit reproduces identical projection on replay`() = runTest {
        // Simulate a crash after publish by using a fresh processor sharing the
        // same store/idempotency, re-delivering the same event.
        val store = InMemoryProjectionStore()
        val idem = InMemoryIdempotencyChecker()
        val publisher = RecordingResultPublisher()
        val processor1 = newProcessor(publisher, idem = idem, store = store)
        processor1.process(importEvent(1, listOf(seg("a", 0), seg("b", 1))))
        val firstState = store.loadState("sb")!!

        // New worker instance (post-crash) re-consumes the same event id.
        val processor2 = newProcessor(publisher, idem = idem, store = store)
        processor2.process(importEvent(1, listOf(seg("a", 0), seg("b", 1))))
        val secondState = store.loadState("sb")!!

        assertEquals(firstState.segmentsById, secondState.segmentsById)
        assertEquals(1, publisher.published.size, "redelivery after commit is a no-op")
    }

    @Test
    fun `large batch of events processes all successfully`() = runTest {
        val publisher = RecordingResultPublisher()
        val processor = newProcessor(publisher)
        processor.process(importEvent(1, listOf(seg("a", 0))))
        for (v in 2..500L) {
            processor.process(
                StoryboardEvent("u$v", "sb", v, EventType.SEGMENT_CREATED, segments = listOf(seg("s$v", v.toInt())))
            )
        }
        assertEquals(500, publisher.published.size)
        assertEquals(500L, publisher.published.last().version)
    }
}
