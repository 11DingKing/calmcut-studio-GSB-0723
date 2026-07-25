package com.calmcut.studio.e2e

import com.calmcut.studio.analysis.AnalysisSettings
import com.calmcut.studio.analysis.DriftDetector
import com.calmcut.studio.analysis.RiskAnalyzer
import com.calmcut.studio.db.DatabaseFactory
import com.calmcut.studio.db.DbWorkerRepository
import com.calmcut.studio.db.EventStore
import com.calmcut.studio.db.storyboardJson
import com.calmcut.studio.domain.EventType
import com.calmcut.studio.domain.Segment
import com.calmcut.studio.domain.StoryboardEvent
import com.calmcut.studio.import.ImportStatus
import com.calmcut.studio.import.StreamingImportService
import com.calmcut.studio.messaging.KafkaEventProducer
import com.calmcut.studio.messaging.OutboxPublisher
import com.calmcut.studio.projection.StoryboardProjector
import com.calmcut.studio.worker.IdempotentProcessor
import com.calmcut.studio.worker.RebuildService
import com.calmcut.studio.worker.ResultPublisher
import com.calmcut.studio.worker.WorkerRepository
import com.calmcut.studio.config.AppConfig
import java.time.Duration
import java.util.Properties
import java.util.UUID
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * End-to-end tests against a real PostgreSQL and a real Redpanda (Kafka API).
 * These validate the durability guarantees that unit tests with fakes cannot:
 * true outbox → Kafka delivery, durable out-of-order buffering across a worker
 * "crash", persistent streaming import with cancel/resume, DLQ replay, projection
 * drift + rebuild, and incremental/full equivalence over the live projection.
 *
 * All tests self-skip when Docker is unavailable.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RealInfraE2ETest {

    private lateinit var db: DatabaseFactory
    private val analyzer = RiskAnalyzer(AnalysisSettings())

    @BeforeAll
    fun setup() {
        assumeTrue(E2EInfra.dockerAvailable, "Docker not available; skipping E2E tests")
        db = DatabaseFactory(E2EInfra.dbConfig()).apply { connect() }
    }

    private fun seg(id: String, order: Int, intensity: Int = 3, reversal: Boolean = false) =
        Segment(id, order, durationMs = 4000, intensity = intensity, strongReversal = reversal)

    private fun repo() = DbWorkerRepository(db)

    private fun captureResults() = object : ResultPublisher {
        val results = mutableListOf<com.calmcut.studio.analysis.AnalysisResult>()
        override suspend fun publish(result: com.calmcut.studio.analysis.AnalysisResult) { results += result }
    }

    // ---- 1. Outbox -> real Kafka/Redpanda delivery -------------------------

    @Test
    fun `outbox publishes events to real redpanda and consumer reads them`() = runBlocking {
        val kafka = E2EInfra.kafkaConfig("outbox-${UUID.randomUUID().toString().take(8)}")
        val eventStore = EventStore(kafka.eventsTopic, db)
        val producer = KafkaEventProducer(kafka)
        val sb = "sb-outbox-${UUID.randomUUID().toString().take(8)}"

        // Write two events via the event store (also enqueues to the outbox).
        db.dbQuery {
            eventStore.append(StoryboardEvent(UUID.randomUUID().toString(), sb, 0, EventType.BATCH_IMPORT, segments = listOf(seg("a", 0), seg("b", 1))), 0)
        }
        db.dbQuery {
            eventStore.append(StoryboardEvent(UUID.randomUUID().toString(), sb, 0, EventType.SEGMENT_CREATED, segments = listOf(seg("c", 2))), 1)
        }

        // Drain the outbox to Redpanda.
        val publisher = OutboxPublisher(db, producer, AppConfig.OutboxConfig(pollIntervalMs = 100, batchSize = 50))
        var drained = 0
        repeat(5) { drained += publisher.drainOnce() }
        producer.close()
        assertTrue(drained >= 2, "outbox should have published at least 2 events, got $drained")

        // Consume from the topic and confirm both events arrived, keyed by storyboard.
        val received = consumeAll(kafka, sb, expected = 2)
        assertEquals(2, received.size)
        assertEquals(listOf(1L, 2L), received.map { it.version }.sorted())
    }

    // ---- 2. Durable out-of-order buffer survives a worker "crash" ----------

    @Test
    fun `out of order events survive worker restart without loss`() = runBlocking {
        val sb = "sb-ooo-${UUID.randomUUID().toString().take(8)}"
        val pub1 = captureResults()
        val worker1 = IdempotentProcessor(analyzer, repo(), pub1)

        // Deliver v3 and v2 first; they must be persisted to pending_event.
        worker1.process(StoryboardEvent("e3-$sb", sb, 3, EventType.SEGMENT_CREATED, segments = listOf(seg("c", 2))))
        worker1.process(StoryboardEvent("e2-$sb", sb, 2, EventType.SEGMENT_CREATED, segments = listOf(seg("b", 1))))
        assertTrue(pub1.results.isEmpty(), "nothing applies before v1")
        assertTrue(repo().nextPending(sb, 2) != null && repo().nextPending(sb, 3) != null, "must be durably buffered in DB")

        // "Crash": brand new processor + repo over the SAME database, then v1 arrives.
        val pub2 = captureResults()
        val worker2 = IdempotentProcessor(analyzer, repo(), pub2)
        worker2.process(StoryboardEvent("e1-$sb", sb, 1, EventType.BATCH_IMPORT, segments = listOf(seg("a", 0))))

        assertEquals(listOf(1L, 2L, 3L), pub2.results.map { it.version })
        assertEquals(3L, repo().lastProcessedVersion(sb))
        assertTrue(repo().nextPending(sb, 2) == null, "durable buffer drained after gap fill")
    }

    // ---- 3. Duplicate messages are idempotent (persisted ledger) -----------

    @Test
    fun `duplicate messages are idempotent against real db`() = runBlocking {
        val sb = "sb-dup-${UUID.randomUUID().toString().take(8)}"
        val pub = captureResults()
        val worker = IdempotentProcessor(analyzer, repo(), pub)
        val event = StoryboardEvent("dup-$sb", sb, 1, EventType.BATCH_IMPORT, segments = listOf(seg("a", 0)))
        worker.process(event)
        worker.process(event)
        worker.process(event)
        assertEquals(1, pub.results.size)
        assertEquals(1L, repo().lastProcessedVersion(sb))
    }

    // ---- 4. Million-scale streaming import (bounded memory) ----------------

    @Test
    fun `million scale streaming import stages and applies without OOM`() = runBlocking {
        val sb = "sb-import-${UUID.randomUUID().toString().take(8)}"
        val eventStore = EventStore("unused", db)
        val imports = StreamingImportService(db, eventStore, chunkSize = 5000)
        val total = 1_000_000

        val jobId = imports.createJob(sb)
        // Lazy flow — never materialises the million segments in memory.
        val source: Flow<Segment> = flow {
            repeat(total) { i -> emit(seg("seg-$i", i, intensity = (i % 5) + 1)) }
        }
        imports.ingest(jobId, source)
        val progress = imports.run(jobId)

        assertEquals(ImportStatus.COMPLETED, progress.status)
        assertEquals(total.toLong(), progress.processed)

        // The projection built from the event log must contain all segments.
        val state = StoryboardProjector.rebuild(sb, eventStore.readLogSuspending(sb))
        assertEquals(total, state.segmentsById.size)
    }

    // ---- 5. Cancel then resume from the DB checkpoint ----------------------

    @Test
    fun `cancel then resume continues from checkpoint`() = runBlocking {
        val sb = "sb-cancel-${UUID.randomUUID().toString().take(8)}"
        val eventStore = EventStore("unused", db)
        val imports = StreamingImportService(db, eventStore, chunkSize = 1000)
        val total = 20_000

        val jobId = imports.createJob(sb)
        imports.ingest(jobId, flow { repeat(total) { i -> emit(seg("s-$i", i)) } })

        // Request cancel almost immediately, then run — it should stop partway.
        imports.requestCancel(jobId)
        val cancelled = imports.run(jobId)
        assertEquals(ImportStatus.CANCELLED, cancelled.status)
        val processedAtCancel = cancelled.processed
        assertTrue(processedAtCancel < total, "cancel should stop before finishing")

        // Resume: must actually continue from the checkpoint and complete.
        val resumed = imports.resume(jobId)!!
        assertEquals(ImportStatus.COMPLETED, resumed.status)
        assertEquals(total.toLong(), resumed.processed)

        val state = StoryboardProjector.rebuild(sb, eventStore.readLogSuspending(sb))
        assertEquals(total, state.segmentsById.size)
    }

    // ---- 6. DLQ replay with audit trail ------------------------------------

    @Test
    fun `poisoned event lands in dlq and replays with audit`() = runBlocking {
        val sb = "sb-dlq-${UUID.randomUUID().toString().take(8)}"
        val failing = object : ResultPublisher {
            var fail = true
            val ok = mutableListOf<com.calmcut.studio.analysis.AnalysisResult>()
            override suspend fun publish(result: com.calmcut.studio.analysis.AnalysisResult) {
                if (fail) throw RuntimeException("boom") else ok += result
            }
        }
        val r = repo()
        val worker = IdempotentProcessor(analyzer, r, failing, maxAttempts = 2)
        val eventId = "dlq-$sb"
        worker.process(StoryboardEvent(eventId, sb, 1, EventType.BATCH_IMPORT, segments = listOf(seg("a", 0))))
        assertEquals(1, r.listDeadLetters().count { !it.replayed })

        failing.fail = false
        val replayed = worker.replayDeadLetter(eventId)
        assertEquals(1, replayed.size)
        assertEquals(1, failing.ok.size)

        val dlq = r.listDeadLetters().single()
        assertTrue(dlq.replayed)
        assertTrue(dlq.replayedAt != null, "replay time must be recorded")
        val audit = r.listAudit(eventId)
        assertTrue(audit.any { it.kind == WorkerRepository.AUDIT_DLQ_REPLAY && it.outcome == WorkerRepository.OUTCOME_SUCCESS })
    }

    // ---- 7. Projection drift + rebuild from event log ----------------------

    @Test
    fun `rebuild from event log heals drift`() = runBlocking {
        val sb = "sb-drift-${UUID.randomUUID().toString().take(8)}"
        val eventStore = EventStore("unused", db)
        val r = repo()
        val pub = captureResults()
        val worker = IdempotentProcessor(analyzer, r, pub)

        // Write events to the log (the source of truth), which assigns versions.
        db.dbQuery {
            eventStore.append(StoryboardEvent(UUID.randomUUID().toString(), sb, 0, EventType.BATCH_IMPORT, segments = listOf(seg("a", 0, intensity = 5), seg("b", 1, intensity = 5))), 0)
        }
        db.dbQuery {
            eventStore.append(StoryboardEvent(UUID.randomUUID().toString(), sb, 0, EventType.SEGMENT_CREATED, segments = listOf(seg("c", 2, intensity = 5))), 1)
        }
        // Feed the same logged events through the worker to build the projection.
        val logged = eventStore.readLogSuspending(sb)
        assertEquals(2, logged.size)
        logged.forEach { worker.process(it) }
        assertEquals(3, r.loadState(sb).segmentsById.size)

        // Corrupt the live projection to simulate drift.
        r.resetStoryboard(sb)
        assertTrue(r.loadState(sb).segmentsById.isEmpty())

        // Rebuild from the event log.
        val rebuilds = RebuildService(eventStore, r, analyzer)
        val result = rebuilds.rebuild(sb)
        assertEquals(2, result.eventsReplayed)

        val healed = r.loadState(sb)
        assertEquals(3, healed.segmentsById.size)
        val fromLog = StoryboardProjector.rebuild(sb, eventStore.readLogSuspending(sb))
        assertFalse(DriftDetector.compareSegments(healed.segmentsById.values, fromLog.segmentsById.values).drifted)

        // Rebuild must be audited.
        assertTrue(r.listAudit(sb).any { it.kind == WorkerRepository.AUDIT_REBUILD && it.outcome == WorkerRepository.OUTCOME_SUCCESS })
    }

    // ---- 8. Incremental == full over the live DB projection ----------------

    @Test
    fun `incremental analysis over live projection equals full recompute`() = runBlocking {
        val sb = "sb-equiv-${UUID.randomUUID().toString().take(8)}"
        val eventStore = EventStore("unused", db)
        val r = repo()
        val pub = captureResults()
        val worker = IdempotentProcessor(analyzer, r, pub)

        // Apply a batch, then a series of incremental single-segment edits.
        worker.process(StoryboardEvent("q1-$sb", sb, 1, EventType.BATCH_IMPORT, segments = (0 until 30).map { seg("s$it", it, intensity = (it % 5) + 1, reversal = it % 4 == 0) }))
        worker.process(StoryboardEvent("q2-$sb", sb, 2, EventType.SEGMENT_UPDATED, segmentId = "s5", segments = listOf(seg("s5", 5, intensity = 5, reversal = true))))
        worker.process(StoryboardEvent("q3-$sb", sb, 3, EventType.SEGMENT_DELETED, segmentId = "s10"))
        worker.process(StoryboardEvent("q4-$sb", sb, 4, EventType.SEGMENT_CREATED, segments = listOf(seg("s99", 99, intensity = 5, reversal = true))))

        // Live incremental result (last published) vs a full recompute of the
        // current projection state.
        val liveResult = pub.results.last()
        val state = r.loadState(sb)
        val full = analyzer.analyzeFull(sb, state.version, state.timeline())
        assertEquals(full.canonical(), liveResult.canonical(), "incremental (worker) must equal full recompute")
    }

    // ---- helper: consume all events for a storyboard from a topic ----------

    private fun consumeAll(kafka: AppConfig.KafkaConfig, storyboardId: String, expected: Int): List<StoryboardEvent> {
        val consumer = KafkaConsumer<String, String>(Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, "e2e-verify-${UUID.randomUUID()}")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        })
        consumer.subscribe(listOf(kafka.eventsTopic))
        val out = mutableListOf<StoryboardEvent>()
        val deadline = System.currentTimeMillis() + 15_000
        try {
            while (out.size < expected && System.currentTimeMillis() < deadline) {
                val records = consumer.poll(Duration.ofMillis(500))
                for (rec in records) {
                    val ev = storyboardJson.decodeFromString(StoryboardEvent.serializer(), rec.value())
                    if (ev.storyboardId == storyboardId) out += ev
                }
            }
        } finally {
            consumer.close()
        }
        return out
    }
}
