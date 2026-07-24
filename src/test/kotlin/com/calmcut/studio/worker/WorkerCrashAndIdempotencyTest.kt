package com.calmcut.studio.worker

import com.calmcut.studio.analysis.AnalysisSettings
import com.calmcut.studio.analysis.RiskAnalyzer
import com.calmcut.studio.domain.EventType
import com.calmcut.studio.domain.Segment
import com.calmcut.studio.domain.StoryboardEvent
import com.calmcut.studio.testutil.InMemoryWorkerRepository
import com.calmcut.studio.testutil.RecordingResultPublisher
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class WorkerCrashAndIdempotencyTest {

    private fun newProcessor(
        publisher: RecordingResultPublisher = RecordingResultPublisher(),
        repo: InMemoryWorkerRepository = InMemoryWorkerRepository(),
        maxAttempts: Int = 3,
    ) = IdempotentProcessor(
        analyzer = RiskAnalyzer(AnalysisSettings()),
        repo = repo,
        publisher = publisher,
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
        val repo = InMemoryWorkerRepository()
        val processor = newProcessor(publisher, repo)
        // Deliver v2 and v3 before v1 — they must be durably buffered.
        processor.process(importEvent(2, listOf(seg("a", 0), seg("b", 1))))
        processor.process(importEvent(3, listOf(seg("a", 0))))
        assertTrue(publisher.published.isEmpty(), "nothing releases before v1")
        assertTrue(repo.nextPending("sb", 2) != null, "v2 must be durably buffered")
        assertTrue(repo.nextPending("sb", 3) != null, "v3 must be durably buffered")

        processor.process(importEvent(1, listOf(seg("a", 0))))
        // v1 applies, then buffered v2 and v3 drain in order.
        assertEquals(listOf(1L, 2L, 3L), publisher.published.map { it.version })
        assertTrue(repo.nextPending("sb", 2) == null, "buffer drained")
    }

    @Test
    fun `crash after buffering but before gap fill does not lose events`() = runTest {
        // Simulate: worker durably buffers v2/v3, then "crashes" (new processor,
        // same repo). The missing v1 arrives at the new worker; buffered events
        // must still be applied — proving the buffer is durable, not in-memory.
        val publisher = RecordingResultPublisher()
        val repo = InMemoryWorkerRepository()
        val worker1 = newProcessor(publisher, repo)
        worker1.process(importEvent(3, listOf(seg("a", 0))))
        worker1.process(importEvent(2, listOf(seg("a", 0), seg("b", 1))))

        // Crash & restart: brand-new processor over the SAME durable repo.
        val worker2 = newProcessor(publisher, repo)
        worker2.process(importEvent(1, listOf(seg("a", 0))))

        assertEquals(listOf(1L, 2L, 3L), publisher.published.map { it.version })
    }

    @Test
    fun `stale events below processed version are ignored`() = runTest {
        val publisher = RecordingResultPublisher()
        val repo = InMemoryWorkerRepository()
        // Bring the storyboard to v5 first.
        val warm = newProcessor(publisher, repo)
        warm.process(importEvent(1, listOf(seg("a", 0))))
        for (v in 2..5L) warm.process(StoryboardEvent("u$v", "sb", v, EventType.SEGMENT_CREATED, segments = listOf(seg("s$v", v.toInt()))))
        val before = publisher.published.size

        val processor = newProcessor(publisher, repo)
        val results = processor.process(importEvent(3, listOf(seg("a", 0))))
        assertTrue(results.isEmpty())
        assertEquals(before, publisher.published.size, "stale event produces no new result")
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
    fun `exhausted retries route event to DLQ and can be replayed with audit`() = runTest {
        val publisher = RecordingResultPublisher().apply { failNextCount = 100 }
        val repo = InMemoryWorkerRepository()
        val processor = newProcessor(publisher, repo, maxAttempts = 2)
        processor.process(importEvent(1, listOf(seg("a", 0))))
        assertEquals(1, repo.listDeadLetters().size, "poisoned event must land in DLQ")

        // Fix the downstream and replay from the DLQ.
        publisher.failNextCount = 0
        val replayed = processor.replayDeadLetter("imp-1")
        assertEquals(1, replayed.size)
        assertEquals(1, publisher.published.size)
        assertTrue(repo.listDeadLetters().single().replayed)
        // Replay must be auditable.
        val audit = processor.auditTrail("imp-1")
        assertTrue(audit.any { it.kind == WorkerRepository.AUDIT_DLQ_REPLAY && it.outcome == WorkerRepository.OUTCOME_SUCCESS })
    }

    @Test
    fun `crash before commit reproduces identical projection on replay`() = runTest {
        val repo = InMemoryWorkerRepository()
        val publisher = RecordingResultPublisher()
        val processor1 = newProcessor(publisher, repo)
        processor1.process(importEvent(1, listOf(seg("a", 0), seg("b", 1))))
        val firstState = repo.loadState("sb")

        // New worker instance (post-crash) re-consumes the same event id.
        val processor2 = newProcessor(publisher, repo)
        processor2.process(importEvent(1, listOf(seg("a", 0), seg("b", 1))))
        val secondState = repo.loadState("sb")

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
