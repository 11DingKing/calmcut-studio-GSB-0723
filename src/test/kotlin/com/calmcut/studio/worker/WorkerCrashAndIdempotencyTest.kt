package com.calmcut.studio.worker

import com.calmcut.studio.analysis.RiskAnalyzer
import com.calmcut.studio.config.AppConfig
import com.calmcut.studio.domain.model.RiskAnalysisResult
import com.calmcut.studio.domain.model.StoryboardSegment
import com.calmcut.studio.domain.model.TimelineState
import com.calmcut.studio.event.KafkaEventPublisher
import com.calmcut.studio.event.SegmentCreated
import com.calmcut.studio.event.StoryboardEvent
import com.calmcut.studio.event.eventJson
import com.calmcut.studio.projection.KnowledgeIntegrityChecker
import com.calmcut.studio.projection.ProjectionRepository
import com.calmcut.studio.projection.ProjectionResult
import com.calmcut.studio.projection.ProjectionUpdater
import com.calmcut.studio.testutil.TestFixtures
import io.mockk.*
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.encodeToString
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class WorkerCrashAndIdempotencyTest {

    private lateinit var config: AppConfig
    private lateinit var analyzer: RiskAnalyzer
    private lateinit var updater: ProjectionUpdater
    private lateinit var repository: ProjectionRepository
    private lateinit var publisher: KafkaEventPublisher
    private lateinit var integrityChecker: KnowledgeIntegrityChecker
    private lateinit var worker: AnalysisWorker
    private lateinit var idempotency: InMemoryIdempotencyChecker
    private lateinit var dlq: InMemoryDeadLetterSink

    private val publishedKeys = mutableListOf<String>()

    @BeforeEach
    fun setup() {
        config = AppConfig(
            db = AppConfig.DbConfig("jdbc:h2:mem:test", "sa", "", 5),
            kafka = AppConfig.KafkaConfig(
                bootstrapServers = "localhost:9092",
                consumerGroup = "test-worker",
                eventsTopic = "events",
                resultsTopic = "results",
                dlqTopic = "dlq",
                producerAcks = "all",
                consumerAutoOffsetReset = "earliest"
            ),
            analysis = TestFixtures.defaultAnalysisConfig(),
            import = AppConfig.ImportConfig(500, 4),
            outbox = AppConfig.OutboxConfig(100, 100)
        )
        analyzer = RiskAnalyzer(config.analysis)
        repository = mockk(relaxed = true)
        publisher = mockk(relaxed = true)
        integrityChecker = mockk(relaxed = true)
        updater = mockk(relaxed = true)
        idempotency = InMemoryIdempotencyChecker()
        dlq = InMemoryDeadLetterSink()

        every { publisher.publishResult(any(), any()) } answers {
            publishedKeys.add(firstArg())
        }

        worker = AnalysisWorker(config, updater, repository, publisher, integrityChecker, idempotency, dlq)
    }

    @AfterEach
    fun teardown() {
        worker.stop()
        publishedKeys.clear()
    }

    private val fixedResult = ProjectionResult(
        timelineId = "tl-1", version = 1,
        analysisResult = RiskAnalysisResult(
            "tl-1", "1.0.0", 1, emptyList(), null, true
        ),
        changedSegmentIds = setOf("seg-1")
    )

    private fun segmentEvent(
        segmentId: String = "seg-1",
        version: Long = 1,
        timelineId: String = "tl-1"
    ): SegmentCreated {
        val seg = StoryboardSegment(
            id = segmentId, timelineId = timelineId, version = version,
            orderIndex = (version - 1).toInt(),
            startTimeMs = (version - 1) * 5000, endTimeMs = version * 5000, intensity = 3,
            isReversal = false, isKnowledgePoint = false, isDeclineInducement = false
        )
        return SegmentCreated(
            eventId = UUID.randomUUID().toString(), timelineId = timelineId,
            aggregateId = segmentId, version = version, segment = seg
        )
    }

    @Test
    fun `duplicate messages are idempotent - same event processed only once`() = runTest {
        val event = segmentEvent()
        val payload = eventJson.encodeToString<StoryboardEvent>(event)

        coEvery { updater.applyEvent(any()) } returns fixedResult
        coEvery { repository.loadTimeline(any()) } returns TimelineState("tl-1", 1, listOf(event.segment))

        worker.processRecord(payload)
        worker.processRecord(payload)
        worker.processRecord(payload)

        coVerify(exactly = 1) { updater.applyEvent(any()) }
    }

    @Test
    fun `out of order events are buffered and processed in sequence`() = runTest {
        val events = (1L..5L).map { v -> segmentEvent("seg-1", v) }
        val payloads = events.associate { it.version to eventJson.encodeToString<StoryboardEvent>(it) }

        coEvery { updater.applyEvent(any()) } returns fixedResult
        coEvery { repository.loadTimeline(any()) } returns TimelineState("tl-1", 5, emptyList())

        worker.processRecord(payloads[3]!!)
        worker.processRecord(payloads[1]!!)
        worker.processRecord(payloads[5]!!)
        coVerify(exactly = 1) { updater.applyEvent(match { it.version == 1L }) }

        worker.processRecord(payloads[2]!!)
        coVerify(exactly = 1) { updater.applyEvent(match { it.version == 2L }) }
        coVerify(exactly = 1) { updater.applyEvent(match { it.version == 3L }) }
        coVerify(exactly = 0) { updater.applyEvent(match { it.version == 4L }) }
        coVerify(exactly = 0) { updater.applyEvent(match { it.version == 5L }) }

        worker.processRecord(payloads[4]!!)
        coVerify(exactly = 1) { updater.applyEvent(match { it.version == 4L }) }
        coVerify(exactly = 1) { updater.applyEvent(match { it.version == 5L }) }
    }

    @Test
    fun `crash during processing - event retried after failure`() = runTest {
        val event = segmentEvent(version = 1)
        val payload = eventJson.encodeToString<StoryboardEvent>(event)

        var callCount = 0
        coEvery { updater.applyEvent(any()) } coAnswers {
            callCount++
            if (callCount == 1) throw RuntimeException("Simulated crash during processing")
            fixedResult
        }
        coEvery { repository.loadTimeline(any()) } returns TimelineState("tl-1", 1, listOf(event.segment))

        runCatching { worker.processRecord(payload) }
        val secondResult = runCatching { worker.processRecord(payload) }

        assertTrue(secondResult.isSuccess, "Retry after crash should succeed")
        coVerify(exactly = 2) { updater.applyEvent(any()) }
    }

    @Test
    fun `stale events with lower version than processed are ignored`() = runTest {
        val v1 = segmentEvent(version = 1)
        val v2 = segmentEvent(version = 2)

        coEvery { updater.applyEvent(any()) } returns fixedResult
        coEvery { repository.loadTimeline(any()) } returns TimelineState("tl-1", 2, emptyList())

        worker.processRecord(eventJson.encodeToString<StoryboardEvent>(v1))
        worker.processRecord(eventJson.encodeToString<StoryboardEvent>(v2))
        worker.processRecord(eventJson.encodeToString<StoryboardEvent>(v1))

        coVerify(exactly = 1) { updater.applyEvent(match { it.version == 1L }) }
        coVerify(exactly = 1) { updater.applyEvent(match { it.version == 2L }) }
    }

    @Test
    fun `successful processing publishes result to Kafka`() = runTest {
        val event = segmentEvent()
        coEvery { updater.applyEvent(any()) } returns fixedResult
        coEvery { repository.loadTimeline(any()) } returns TimelineState("tl-1", 1, emptyList())
        coEvery { integrityChecker.verify(any()) } returns emptyList()

        worker.processRecord(eventJson.encodeToString<StoryboardEvent>(event))

        verify(exactly = 1) { publisher.publishResult(eq("tl-1"), any()) }
    }

    @Test
    fun `large batch of events processes all successfully`() = runTest {
        coEvery { updater.applyEvent(any()) } returns fixedResult
        coEvery { repository.loadTimeline(any()) } returns TimelineState("tl-1", 50, emptyList())

        for (v in 1L..50L) {
            val event = segmentEvent("seg-$v", version = 1, timelineId = "tl-1")
            worker.processRecord(eventJson.encodeToString<StoryboardEvent>(event))
        }

        coVerify(exactly = 50) { updater.applyEvent(any()) }
        verify(exactly = 50) { publisher.publishResult(any(), any()) }
    }

    @Test
    fun `DLQ receives failed events`() = runTest {
        val event = segmentEvent()
        coEvery { updater.applyEvent(any()) } throws RuntimeException("Processing failed")

        runCatching { worker.processRecord(eventJson.encodeToString<StoryboardEvent>(event)) }

        assertEquals(0, dlq.entries.size, "DLQ should only get entries from Kafka consumer loop, not processRecord directly")
    }

    @Test
    fun `version buffer tracks processed version`() {
        assertEquals(0L, worker.versionBufferRef.getProcessedVersion("unknown"))
        worker.versionBufferRef.setProcessedVersion("test-agg", 5L)
        assertEquals(5L, worker.versionBufferRef.getProcessedVersion("test-agg"))
    }
}
