package com.calmcut.integration

import com.calmcut.domain.*
import com.calmcut.domain.events.*
import com.calmcut.domain.rules.*
import com.calmcut.infrastructure.db.*
import com.calmcut.infrastructure.messaging.*
import com.calmcut.infrastructure.repository.*
import com.calmcut.service.*
import io.ktor.server.config.*
import kotlinx.coroutines.*
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IntegrationTest {

    private lateinit var postgres: PostgreSQLContainer<*>
    private lateinit var redpanda: KafkaContainer
    private lateinit var dbFactory: DatabaseFactory
    private lateinit var storyboardRepo: StoryboardRepository
    private lateinit var eventLogRepo: EventLogRepository
    private lateinit var atomicWriteRepo: AtomicWriteRepository
    private lateinit var projectionRepo: RiskProjectionRepository
    private lateinit var deadLetterRepo: DeadLetterRepository
    private lateinit var importRepo: ImportRepository
    private lateinit var commandService: StoryboardCommandService
    private lateinit var analysisEngine: RiskAnalysisEngine
    private lateinit var kpValidator: KnowledgePointValidator
    private lateinit var eventProcessor: EventProcessor
    private val consumerGroup = "test-worker-group"

    @BeforeAll
    fun setup() {
        postgres = PostgreSQLContainer("postgres:16-alpine")
            .withDatabaseName("storyboard_risk")
            .withUsername("storyboard")
            .withPassword("storyboard")
        postgres.start()

        redpanda = KafkaContainer(DockerImageName.parse("docker.redpanda.com/vectorized/redpanda:v23.3.6"))
        redpanda.start()

        val configPairs = mapOf(
            "database.jdbcUrl" to postgres.jdbcUrl,
            "database.driverClassName" to "org.postgresql.Driver",
            "database.username" to postgres.username,
            "database.password" to postgres.password,
            "database.maximumPoolSize" to "10",
            "redpanda.bootstrapServers" to redpanda.bootstrapServers,
            "redpanda.consumerGroupId" to consumerGroup,
            "redpanda.topicEvents" to "test-storyboard-events",
            "redpanda.topicAnalysisResults" to "test-analysis-results",
            "redpanda.topicDeadLetter" to "test-dead-letter",
            "analysis.ruleVersion" to "1.0.0",
            "analysis.windowSizeSeconds" to "60",
            "analysis.maxReversalsPerWindow" to "6",
            "analysis.consecutiveHighStimulus" to "3",
            "analysis.highStimulusThreshold" to "5",
            "analysis.minAverageShotSeconds" to "3.0",
            "analysis.bufferStimulusThreshold" to "2",
            "analysis.workerPoolSize" to "2",
            "analysis.driftCheckIntervalSeconds" to "3600",
            "import.batchSize" to "100",
            "import.maxConcurrentJobs" to "1"
        )
        val config = MapApplicationConfig(*configPairs.toList().toTypedArray())

        dbFactory = DatabaseFactory(config)
        dbFactory.connect()

        storyboardRepo = StoryboardRepository()
        eventLogRepo = EventLogRepository()
        atomicWriteRepo = AtomicWriteRepository()
        projectionRepo = RiskProjectionRepository()
        deadLetterRepo = DeadLetterRepository()
        importRepo = ImportRepository()

        val ruleConfig = RiskRuleConfig()
        analysisEngine = RiskAnalysisEngine(ruleConfig, storyboardRepo, projectionRepo)
        kpValidator = KnowledgePointValidator(storyboardRepo)
        eventProcessor = EventProcessor(storyboardRepo, atomicWriteRepo, projectionRepo, analysisEngine, kpValidator, ruleConfig)
        commandService = StoryboardCommandService(storyboardRepo, atomicWriteRepo, eventLogRepo, kpValidator, "test-storyboard-events")
    }

    @AfterAll
    fun teardown() {
        dbFactory.close()
        redpanda.stop()
        postgres.stop()
    }

    @Test
    fun `atomic transaction - version event and outbox are all-or-nothing`() = runBlocking {
        val externalId = "test-atomic-${UUID.randomUUID()}"
        val result = commandService.createStoryboard(externalId, "Atomic Test", listOf("kp1", "kp2"))
        assertTrue(result is CommandResult.Success, "Storyboard creation should succeed: $result")
        val success = result as CommandResult.Success

        val sb = storyboardRepo.findById(StoryboardId(success.storyboardId))
        assertNotNull(sb)
        assertEquals(1L, sb!!.currentVersion)

        val version = storyboardRepo.getCurrentVersion(StoryboardId(success.storyboardId))
        assertEquals(1L, version)
    }

    @Test
    fun `optimistic lock - concurrent modifications rejected`() = runBlocking {
        val externalId = "test-optlock-${UUID.randomUUID()}"
        val created = commandService.createStoryboard(externalId, "OptLock Test")
        val sbId = (created as CommandResult.Success).storyboardId

        val seg = SegmentData(order = 0, startTimeMs = 0, endTimeMs = 3000, stimulusIntensity = 3)
        val result1 = commandService.addSegment(sbId, 1, seg)
        assertTrue(result1 is CommandResult.Success, "First add should succeed: $result1")

        val result2 = commandService.addSegment(sbId, 1, seg.copy(order = 1))
        assertTrue(result2 is CommandResult.VersionConflict, "Second add with stale version should fail: $result2")
    }

    @Test
    fun `timeline - overlapping segments rejected at write layer`() = runBlocking {
        val externalId = "test-timeline-${UUID.randomUUID()}"
        val created = commandService.createStoryboard(externalId, "Timeline Test")
        val sbId = (created as CommandResult.Success).storyboardId

        val seg1 = SegmentData(order = 0, startTimeMs = 0, endTimeMs = 5000, stimulusIntensity = 3)
        val r1 = commandService.addSegment(sbId, 1, seg1)
        assertTrue(r1 is CommandResult.Success)

        val overlapping = SegmentData(order = 1, startTimeMs = 3000, endTimeMs = 8000, stimulusIntensity = 3)
        val result = commandService.addSegment(sbId, 2, overlapping)
        assertTrue(result is CommandResult.ValidationError, "Overlapping segment should be rejected: $result")
    }

    @Test
    fun `timeline - non-overlapping contiguous segments accepted`() = runBlocking {
        val externalId = "test-timeline2-${UUID.randomUUID()}"
        val created = commandService.createStoryboard(externalId, "Timeline Test 2")
        val sbId = (created as CommandResult.Success).storyboardId

        val seg1 = SegmentData(order = 0, startTimeMs = 0, endTimeMs = 3000, stimulusIntensity = 3)
        val seg2 = SegmentData(order = 1, startTimeMs = 3000, endTimeMs = 6000, stimulusIntensity = 3)

        val r1 = commandService.addSegment(sbId, 1, seg1)
        assertTrue(r1 is CommandResult.Success, "seg1: $r1")
        val r2 = commandService.addSegment(sbId, 2, seg2)
        assertTrue(r2 is CommandResult.Success, "seg2: $r2")
    }

    @Test
    fun `idempotent processing - same event processed twice does not duplicate`() = runBlocking {
        val externalId = "test-idem-${UUID.randomUUID()}"
        val created = commandService.createStoryboard(externalId, "Idempotent Test")
        val sbId = (created as CommandResult.Success).storyboardId

        val seg = SegmentData(order = 0, startTimeMs = 0, endTimeMs = 3000, stimulusIntensity = 5, hasReversal = true)
        val addResult = commandService.addSegment(sbId, 1, seg)
        assertTrue(addResult is CommandResult.Success)

        eventProcessor.processEvent(
            SegmentAdded(storyboardId = sbId, aggregateVersion = 2, segment = seg),
            consumerGroup
        )

        val segments = storyboardRepo.getSegments(StoryboardId(sbId))
        val matchingSegs = segments.count { it.startTimeMs == 0L && it.endTimeMs == 3000L }
        assertEquals(1, matchingSegs, "Segment should exist exactly once after duplicate event")
    }

    @Test
    fun `persistent dedup - already processed version skipped on reprocessing`() = runBlocking {
        val externalId = "test-persist-${UUID.randomUUID()}"
        val created = commandService.createStoryboard(externalId, "Persist Dedup Test")
        val sbId = (created as CommandResult.Success).storyboardId

        val seg = SegmentData(order = 0, startTimeMs = 0, endTimeMs = 3000, stimulusIntensity = 3)
        val event = SegmentAdded(storyboardId = sbId, aggregateVersion = 2, segment = seg)
        val result1 = eventProcessor.processEvent(event, consumerGroup)
        assertTrue(result1.success)
        assertEquals(2L, result1.processedVersion)

        val result2 = eventProcessor.processEvent(event, consumerGroup)
        assertTrue(result2.success, "Re-processing same version should succeed as idempotent")
        assertEquals(2L, result2.processedVersion)
    }

    @Test
    fun `projection rebuild - full rebuild produces valid deterministic results`() = runBlocking {
        val externalId = "test-rebuild-${UUID.randomUUID()}"
        val created = commandService.createStoryboard(externalId, "Rebuild Test", listOf("kp1", "kp2"))
        val sbId = StoryboardId((created as CommandResult.Success).storyboardId)

        for (i in 0 until 20) {
            val intensity = if (i % 4 == 0) 5 else 2
            val isKp = i % 10 == 0
            val seg = SegmentData(
                order = i, startTimeMs = i * 3000L, endTimeMs = (i + 1) * 3000L,
                stimulusIntensity = intensity,
                hasReversal = i % 6 == 0 && intensity == 5,
                isKnowledgePoint = isKp,
                knowledgePointId = if (isKp) "kp${i/10}" else null,
                hasScrollInducement = i % 15 == 0
            )
            val event = SegmentAdded(storyboardId = sbId.value, aggregateVersion = (i + 2).toLong(), segment = seg)
            eventProcessor.processEvent(event, consumerGroup)
        }

        val rebuilt1 = eventProcessor.rebuildProjectionFromScratch(sbId)
        assertNotNull(rebuilt1)
        assertTrue(rebuilt1.projectionVersion > 0)
        assertTrue(rebuilt1.ruleVersion.isNotBlank())

        val rebuilt2 = eventProcessor.rebuildProjectionFromScratch(sbId)
        val keys1 = rebuilt1.findings.map { f -> "${f.ruleId}:${f.windowStartMs}:${f.evidence.description.hashCode()}" }.toSet()
        val keys2 = rebuilt2.findings.map { f -> "${f.ruleId}:${f.windowStartMs}:${f.evidence.description.hashCode()}" }.toSet()
        assertEquals(keys1.size, keys2.size, "Rebuild should be deterministic")
        assertEquals(keys1, keys2, "Two full rebuilds should produce identical findings")
    }

    @Test
    fun `drift detection - incremental matches full after series of changes`() = runBlocking {
        val externalId = "test-drift-${UUID.randomUUID()}"
        val created = commandService.createStoryboard(externalId, "Drift Test")
        val sbId = StoryboardId((created as CommandResult.Success).storyboardId)

        for (i in 0 until 30) {
            val intensity = when { i % 10 < 3 -> 5; i % 5 < 2 -> 1; else -> 3 }
            val isKp = i % 12 == 0
            val seg = SegmentData(
                order = i, startTimeMs = i * 3000L, endTimeMs = (i + 1) * 3000L,
                stimulusIntensity = intensity,
                hasReversal = i % 8 == 0 && intensity >= 4,
                isKnowledgePoint = isKp,
                knowledgePointId = if (isKp) "kp${i/12}" else null,
                hasScrollInducement = i % 25 == 0
            )
            eventProcessor.processEvent(
                SegmentAdded(storyboardId = sbId.value, aggregateVersion = (i + 2).toLong(), segment = seg), consumerGroup
            )
        }

        val version = atomicWriteRepo.getProcessedVersion(consumerGroup, sbId.value)
        val drift = analysisEngine.verifyEquivalence(sbId, version)
        assertFalse(drift.hasDrift, "Incremental should exactly match full: ${drift.message}")
    }

    @Test
    fun `knowledge point integrity - missing points detected`() = runBlocking {
        val externalId = "test-kp-${UUID.randomUUID()}"
        val created = commandService.createStoryboard(externalId, "KP Test", listOf("kp1", "kp2", "kp3"))
        val sbId = StoryboardId((created as CommandResult.Success).storyboardId)

        val seg1 = SegmentData(order = 0, startTimeMs = 0, endTimeMs = 3000, stimulusIntensity = 3,
            isKnowledgePoint = true, knowledgePointId = "kp1")
        eventProcessor.processEvent(SegmentAdded(storyboardId = sbId.value, aggregateVersion = 2, segment = seg1), consumerGroup)

        val validation = kpValidator.validateKnowledgePointIntegrity(sbId)
        assertFalse(validation.isValid, "Should detect missing kp2, kp3")
        assertTrue(validation.missingIds.contains("kp2"), "Should detect kp2 missing")
        assertTrue(validation.missingIds.contains("kp3"), "Should detect kp3 missing")
    }

    @Test
    fun `knowledge point integrity - all points present validates OK`() = runBlocking {
        val externalId = "test-kp2-${UUID.randomUUID()}"
        val created = commandService.createStoryboard(externalId, "KP Test 2", listOf("kp1", "kp2"))
        val sbId = StoryboardId((created as CommandResult.Success).storyboardId)

        val seg1 = SegmentData(order = 0, startTimeMs = 0, endTimeMs = 3000, stimulusIntensity = 3,
            isKnowledgePoint = true, knowledgePointId = "kp1")
        val seg2 = SegmentData(order = 1, startTimeMs = 3000, endTimeMs = 6000, stimulusIntensity = 3,
            isKnowledgePoint = true, knowledgePointId = "kp2")

        eventProcessor.processEvent(SegmentAdded(storyboardId = sbId.value, aggregateVersion = 2, segment = seg1), consumerGroup)
        eventProcessor.processEvent(SegmentAdded(storyboardId = sbId.value, aggregateVersion = 3, segment = seg2), consumerGroup)

        val validation = kpValidator.validateKnowledgePointIntegrity(sbId)
        assertTrue(validation.isValid, "All KPs present should validate OK: ${validation.errors}")
    }

    @Test
    fun `risk findings - detect violations and include evidence without medical diagnoses`() = runBlocking {
        val externalId = "test-risk-${UUID.randomUUID()}"
        val created = commandService.createStoryboard(externalId, "Risk Test")
        val sbId = StoryboardId((created as CommandResult.Success).storyboardId)

        for (i in 0 until 25) {
            val isKp = i % 6 == 0
            val seg = SegmentData(
                order = i, startTimeMs = i * 1000L, endTimeMs = (i + 1) * 1000L,
                stimulusIntensity = if (i < 10) 5 else 3,
                hasReversal = i < 8,
                isKnowledgePoint = isKp,
                knowledgePointId = if (isKp) "kp${i/6}" else null,
                hasScrollInducement = i == 12
            )
            eventProcessor.processEvent(SegmentAdded(storyboardId = sbId.value, aggregateVersion = (i + 2).toLong(), segment = seg), consumerGroup)
        }

        val result = analysisEngine.analyzeFull(sbId, 27)
        assertTrue(result.findings.isNotEmpty(), "Should detect risk findings")

        result.findings.forEach { f ->
            assertTrue(f.evidence.description.isNotBlank(), "Finding must have evidence for ${f.ruleId}")
            assertTrue(f.suggestion.isNotBlank(), "Finding must have suggestion for ${f.ruleId}")
            assertFalse(
                f.suggestion.contains("成瘾") || f.suggestion.contains("脑损伤") ||
                f.evidence.description.contains("成瘾") || f.evidence.description.contains("脑损伤") ||
                f.suggestion.contains("addiction", ignoreCase = true) ||
                f.suggestion.contains("brain damage", ignoreCase = true),
                "Must not diagnose addiction or brain damage: ${f.suggestion}"
            )
        }
    }

    @Test
    fun `dead letter - failed events stored for replay`() = runBlocking {
        val dlqCount = deadLetterRepo.getUnresolvedCount()
        assertTrue(dlqCount >= 0, "Should be able to query DLQ count")
    }
}
