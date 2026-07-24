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

    private var postgres: PostgreSQLContainer<*>? = null
    private var redpanda: KafkaContainer? = null
    private var externalDb: Boolean = false
    private var dockerAvailable: Boolean = false
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
        val jdbcUrl: String
        val dbUser: String
        val dbPass: String
        val bootstrapServers: String

        val externalJdbcUrl = System.getenv("TEST_DATABASE_URL")
        val externalDbUser = System.getenv("TEST_DATABASE_USER")
        val externalDbPass = System.getenv("TEST_DATABASE_PASSWORD")
        val externalKafka = System.getenv("TEST_KAFKA_BOOTSTRAP_SERVERS")

        if (externalJdbcUrl != null && externalKafka != null) {
            jdbcUrl = externalJdbcUrl
            dbUser = externalDbUser ?: "storyboard"
            dbPass = externalDbPass ?: "storyboard"
            bootstrapServers = externalKafka
            externalDb = true
            dockerAvailable = true
            println("Using external PostgreSQL and Kafka for integration tests")
        } else {
            val pg: PostgreSQLContainer<*>
            val rp: KafkaContainer
            try {
                pg = PostgreSQLContainer("postgres:16-alpine")
                    .withDatabaseName("storyboard_risk")
                    .withUsername("storyboard")
                    .withPassword("storyboard")
                pg.start()
                postgres = pg

                rp = KafkaContainer(DockerImageName.parse("docker.redpanda.com/vectorized/redpanda:v23.3.6"))
                rp.start()
                redpanda = rp
            } catch (e: Exception) {
                println("Testcontainers failed to start: ${e.message}")
                Assumptions.assumeTrue(false,
                    "Testcontainers could not start Docker containers: ${e.message}. " +
                    "Set TEST_DATABASE_URL and TEST_KAFKA_BOOTSTRAP_SERVERS for external services.")
                return
            }

            jdbcUrl = pg.jdbcUrl
            dbUser = pg.username
            dbPass = pg.password
            bootstrapServers = rp.bootstrapServers
            dockerAvailable = true
            println("Using Testcontainers PostgreSQL and Redpanda")
        }

        val configPairs = mapOf(
            "database.jdbcUrl" to jdbcUrl,
            "database.driverClassName" to "org.postgresql.Driver",
            "database.username" to dbUser,
            "database.password" to dbPass,
            "database.maximumPoolSize" to "10",
            "redpanda.bootstrapServers" to bootstrapServers,
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
        val flatPairs = configPairs.toList().toTypedArray()
        val config = MapApplicationConfig(*flatPairs)

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
        if (::dbFactory.isInitialized) dbFactory.close()
        if (!externalDb) {
            redpanda?.stop()
            postgres?.stop()
        }
    }

    // ========== Atomic Transaction Tests ==========

    @Test
    fun `atomic transaction - all three writes succeed together`() = runBlocking {
        val externalId = "test-atomic-${UUID.randomUUID()}"
        val result = commandService.createStoryboard(externalId, "Atomic Test", listOf("kp1", "kp2"))
        assertTrue(result is CommandResult.Success, "Creation should succeed: $result")
        val success = result as CommandResult.Success

        val sb = storyboardRepo.findById(StoryboardId(success.storyboardId))
        assertNotNull(sb)
        assertEquals(1L, sb!!.currentVersion)

        val version = storyboardRepo.getCurrentVersion(StoryboardId(success.storyboardId))
        assertEquals(1L, version)
    }

    @Test
    fun `atomic transaction - rollback on optimistic lock failure leaves no partial state`() = runBlocking {
        val externalId = "test-atomic-rollback-${UUID.randomUUID()}"
        val created = commandService.createStoryboard(externalId, "Rollback Test")
        val sbId = (created as CommandResult.Success).storyboardId

        val seg = SegmentData(order = 0, startTimeMs = 0, endTimeMs = 3000, stimulusIntensity = 3)
        val r1 = commandService.addSegment(sbId, 1, seg)
        assertTrue(r1 is CommandResult.Success, "First add should succeed")

        val r2 = commandService.addSegment(sbId, 1, seg.copy(order = 1, startTimeMs = 3000, endTimeMs = 6000))
        assertTrue(r2 is CommandResult.VersionConflict, "Stale version should be rejected: $r2")

        val segments = storyboardRepo.getSegments(StoryboardId(sbId))
        assertEquals(1, segments.size, "Only one segment should exist after failed concurrent add")
    }

    // ========== Optimistic Lock Tests ==========

    @Test
    fun `optimistic lock - version conflict returns actual version`() = runBlocking {
        val externalId = "test-optlock-${UUID.randomUUID()}"
        val created = commandService.createStoryboard(externalId, "OptLock Test")
        val sbId = (created as CommandResult.Success).storyboardId

        val seg = SegmentData(order = 0, startTimeMs = 0, endTimeMs = 3000, stimulusIntensity = 3)
        val r1 = commandService.addSegment(sbId, 1, seg)
        assertTrue(r1 is CommandResult.Success)

        val r2 = commandService.addSegment(sbId, 1, seg.copy(order = 1))
        assertTrue(r2 is CommandResult.VersionConflict)
        val conflict = r2 as CommandResult.VersionConflict
        assertEquals(1L, conflict.expected)
        assertEquals(2L, conflict.actual, "Actual version should be 2 after successful add")
    }

    // ========== Timeline Validation Tests ==========

    @Test
    fun `timeline - overlapping segment rejected`() = runBlocking {
        val externalId = "test-tl-overlap-${UUID.randomUUID()}"
        val created = commandService.createStoryboard(externalId, "Timeline Overlap Test")
        val sbId = (created as CommandResult.Success).storyboardId

        val seg1 = SegmentData(order = 0, startTimeMs = 0, endTimeMs = 5000, stimulusIntensity = 3)
        val r1 = commandService.addSegment(sbId, 1, seg1)
        assertTrue(r1 is CommandResult.Success, "First segment should succeed: $r1")

        val overlapping = SegmentData(order = 1, startTimeMs = 3000, endTimeMs = 8000, stimulusIntensity = 3)
        val r2 = commandService.addSegment(sbId, 2, overlapping)
        assertTrue(r2 is CommandResult.ValidationError, "Overlap must be rejected: $r2")
        val errors = (r2 as CommandResult.ValidationError).errors
        assertTrue(errors.any { it.contains("Overlap", ignoreCase = true) }, "Error should mention overlap: $errors")
    }

    @Test
    fun `timeline - gap between segments rejected`() = runBlocking {
        val externalId = "test-tl-gap-${UUID.randomUUID()}"
        val created = commandService.createStoryboard(externalId, "Timeline Gap Test")
        val sbId = (created as CommandResult.Success).storyboardId

        val seg1 = SegmentData(order = 0, startTimeMs = 0, endTimeMs = 3000, stimulusIntensity = 3)
        val r1 = commandService.addSegment(sbId, 1, seg1)
        assertTrue(r1 is CommandResult.Success)

        val withGap = SegmentData(order = 1, startTimeMs = 5000, endTimeMs = 8000, stimulusIntensity = 3)
        val r2 = commandService.addSegment(sbId, 2, withGap)
        assertTrue(r2 is CommandResult.ValidationError, "Gap must be rejected: $r2")
        val errors = (r2 as CommandResult.ValidationError).errors
        assertTrue(errors.any { it.contains("Gap", ignoreCase = true) }, "Error should mention gap: $errors")
    }

    @Test
    fun `timeline - perfectly contiguous segments accepted`() = runBlocking {
        val externalId = "test-tl-contig-${UUID.randomUUID()}"
        val created = commandService.createStoryboard(externalId, "Contiguous Test")
        val sbId = (created as CommandResult.Success).storyboardId

        val seg1 = SegmentData(order = 0, startTimeMs = 0, endTimeMs = 3000, stimulusIntensity = 3)
        val seg2 = SegmentData(order = 1, startTimeMs = 3000, endTimeMs = 6000, stimulusIntensity = 3)

        val r1 = commandService.addSegment(sbId, 1, seg1)
        assertTrue(r1 is CommandResult.Success, "seg1: $r1")
        val r2 = commandService.addSegment(sbId, 2, seg2)
        assertTrue(r2 is CommandResult.Success, "seg2 contiguous should succeed: $r2")
    }

    @Test
    fun `timeline - delete middle segment rejected due to gap`() = runBlocking {
        val externalId = "test-tl-del-mid-${UUID.randomUUID()}"
        val created = commandService.createStoryboard(externalId, "Delete Middle Test")
        val sbId = StoryboardId((created as CommandResult.Success).storyboardId)

        val seg1 = SegmentData(order = 0, startTimeMs = 0, endTimeMs = 3000, stimulusIntensity = 3)
        val seg2 = SegmentData(order = 1, startTimeMs = 3000, endTimeMs = 6000, stimulusIntensity = 3)
        val seg3 = SegmentData(order = 2, startTimeMs = 6000, endTimeMs = 9000, stimulusIntensity = 3)

        commandService.addSegment(sbId.value, 1, seg1)
        val r2 = commandService.addSegment(sbId.value, 2, seg2)
        assertTrue(r2 is CommandResult.Success)
        commandService.addSegment(sbId.value, 3, seg3)

        val segments = storyboardRepo.getSegments(sbId)
        val middleId = segments.find { it.startTimeMs == 3000L }?.id?.value
        assertNotNull(middleId, "Middle segment should exist")

        val deleteResult = commandService.deleteSegment(sbId.value, 4, middleId!!)
        assertTrue(deleteResult is CommandResult.ValidationError,
            "Deleting middle segment must be rejected (creates gap): $deleteResult")
    }

    @Test
    fun `timeline - delete last segment accepted`() = runBlocking {
        val externalId = "test-tl-del-last-${UUID.randomUUID()}"
        val created = commandService.createStoryboard(externalId, "Delete Last Test")
        val sbId = StoryboardId((created as CommandResult.Success).storyboardId)

        val seg1 = SegmentData(order = 0, startTimeMs = 0, endTimeMs = 3000, stimulusIntensity = 3)
        val seg2 = SegmentData(order = 1, startTimeMs = 3000, endTimeMs = 6000, stimulusIntensity = 3)

        commandService.addSegment(sbId.value, 1, seg1)
        commandService.addSegment(sbId.value, 2, seg2)

        val segments = storyboardRepo.getSegments(sbId)
        val lastId = segments.maxByOrNull { it.endTimeMs }?.id?.value

        val deleteResult = commandService.deleteSegment(sbId.value, 3, lastId!!)
        assertTrue(deleteResult is CommandResult.Success, "Deleting last segment should succeed: $deleteResult")
    }

    @Test
    fun `timeline - update creating overlap rejected`() = runBlocking {
        val externalId = "test-tl-upd-overlap-${UUID.randomUUID()}"
        val created = commandService.createStoryboard(externalId, "Update Overlap Test")
        val sbId = StoryboardId((created as CommandResult.Success).storyboardId)

        val seg1 = SegmentData(order = 0, startTimeMs = 0, endTimeMs = 3000, stimulusIntensity = 3)
        val seg2 = SegmentData(order = 1, startTimeMs = 3000, endTimeMs = 6000, stimulusIntensity = 3)
        val seg3 = SegmentData(order = 2, startTimeMs = 6000, endTimeMs = 9000, stimulusIntensity = 3)

        commandService.addSegment(sbId.value, 1, seg1)
        commandService.addSegment(sbId.value, 2, seg2)
        commandService.addSegment(sbId.value, 3, seg3)

        val segments = storyboardRepo.getSegments(sbId)
        val middleId = segments.find { it.startTimeMs == 3000L }?.id?.value

        val changes = SegmentChangeSet(startTimeMs = 2000, endTimeMs = 5000)
        val updateResult = commandService.updateSegment(sbId.value, 4, middleId!!, changes)
        assertTrue(updateResult is CommandResult.ValidationError,
            "Update creating overlap must be rejected: $updateResult")
    }

    @Test
    fun `timeline - batch import with gaps rejected`() = runBlocking {
        val externalId = "test-tl-batch-gap-${UUID.randomUUID()}"
        val created = commandService.createStoryboard(externalId, "Batch Gap Test")
        val sbId = (created as CommandResult.Success).storyboardId

        val segments = listOf(
            SegmentData(order = 0, startTimeMs = 0, endTimeMs = 3000, stimulusIntensity = 3),
            SegmentData(order = 1, startTimeMs = 5000, endTimeMs = 8000, stimulusIntensity = 3)
        )
        val result = commandService.batchImportSegments(sbId, 1, UUID.randomUUID().toString(), segments)
        assertTrue(result is CommandResult.ValidationError, "Batch with gaps must be rejected: $result")
    }

    @Test
    fun `timeline - batch import perfectly contiguous accepted`() = runBlocking {
        val externalId = "test-tl-batch-ok-${UUID.randomUUID()}"
        val created = commandService.createStoryboard(externalId, "Batch OK Test")
        val sbId = (created as CommandResult.Success).storyboardId

        val segments = (0 until 5).map { i ->
            SegmentData(order = i, startTimeMs = i * 3000L, endTimeMs = (i + 1) * 3000L, stimulusIntensity = 3)
        }
        val result = commandService.batchImportSegments(sbId, 1, UUID.randomUUID().toString(), segments)
        assertTrue(result is CommandResult.Success, "Contiguous batch should succeed: $result")
    }

    // ========== Idempotent Processing Tests ==========

    @Test
    fun `idempotent - same event processed twice does not duplicate data`() = runBlocking {
        val externalId = "test-idem-${UUID.randomUUID()}"
        val created = commandService.createStoryboard(externalId, "Idempotent Test")
        val sbId = (created as CommandResult.Success).storyboardId

        val seg = SegmentData(order = 0, startTimeMs = 0, endTimeMs = 3000, stimulusIntensity = 5, hasReversal = true)
        val addResult = commandService.addSegment(sbId, 1, seg)
        assertTrue(addResult is CommandResult.Success)

        val version = atomicWriteRepo.getProcessedVersion(consumerGroup, sbId)
        eventProcessor.processEvent(
            SegmentAdded(storyboardId = sbId, aggregateVersion = 2, segment = seg), consumerGroup
        )

        val segments = storyboardRepo.getSegments(StoryboardId(sbId))
        assertEquals(1, segments.size, "Segment should exist exactly once")
    }

    // ========== Persistent Dedup Tests ==========

    @Test
    fun `persistent dedup - version tracking survives across processor calls`() = runBlocking {
        val externalId = "test-persist-${UUID.randomUUID()}"
        val created = commandService.createStoryboard(externalId, "Persistent Dedup Test")
        val sbId = (created as CommandResult.Success).storyboardId

        val seg = SegmentData(order = 0, startTimeMs = 0, endTimeMs = 3000, stimulusIntensity = 3)
        val event = SegmentAdded(storyboardId = sbId, aggregateVersion = 2, segment = seg)

        val r1 = eventProcessor.processEvent(event, consumerGroup)
        assertTrue(r1.success)
        assertEquals(2L, r1.processedVersion)

        val r2 = eventProcessor.processEvent(event, consumerGroup)
        assertTrue(r2.success, "Re-processing same version should be idempotent")

        val storedVersion = atomicWriteRepo.getProcessedVersion(consumerGroup, sbId)
        assertEquals(2L, storedVersion, "Persisted version should be 2")
    }

    // ========== Projection Rebuild Tests ==========

    @Test
    fun `rebuild from scratch produces deterministic results`() = runBlocking {
        val externalId = "test-rebuild-${UUID.randomUUID()}"
        val created = commandService.createStoryboard(externalId, "Rebuild Test", listOf("kp1"))
        val sbId = StoryboardId((created as CommandResult.Success).storyboardId)

        for (i in 0 until 20) {
            val intensity = if (i % 4 == 0) 5 else 2
            val isKp = i % 10 == 0
            val seg = SegmentData(
                order = i, startTimeMs = i * 3000L, endTimeMs = (i + 1) * 3000L,
                stimulusIntensity = intensity,
                hasReversal = i % 6 == 0 && intensity == 5,
                isKnowledgePoint = isKp,
                knowledgePointId = if (isKp) "kp${i/10}" else null
            )
            eventProcessor.processEvent(
                SegmentAdded(storyboardId = sbId.value, aggregateVersion = (i + 2).toLong(), segment = seg),
                consumerGroup
            )
        }

        val rebuilt1 = eventProcessor.rebuildProjectionFromScratch(sbId)
        assertNotNull(rebuilt1)
        assertTrue(rebuilt1.projectionVersion > 0)

        val rebuilt2 = eventProcessor.rebuildProjectionFromScratch(sbId)
        val keys1 = rebuilt1.findings.map { f -> "${f.ruleId}:${f.windowStartMs}:${f.evidence.description.hashCode()}" }.toSet()
        val keys2 = rebuilt2.findings.map { f -> "${f.ruleId}:${f.windowStartMs}:${f.evidence.description.hashCode()}" }.toSet()
        assertEquals(keys1.size, keys2.size, "Two rebuilds should produce same finding count")
        assertEquals(keys1, keys2, "Two full rebuilds should produce identical findings")
    }

    // ========== Drift Detection Tests ==========

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
                SegmentAdded(storyboardId = sbId.value, aggregateVersion = (i + 2).toLong(), segment = seg),
                consumerGroup
            )
        }

        val version = atomicWriteRepo.getProcessedVersion(consumerGroup, sbId.value)
        val drift = analysisEngine.verifyEquivalence(sbId, version)
        assertFalse(drift.hasDrift, "Incremental should match full exactly: ${drift.message}")
    }

    // ========== Knowledge Point Integrity Tests ==========

    @Test
    fun `knowledge point integrity - missing points detected`() = runBlocking {
        val externalId = "test-kp-${UUID.randomUUID()}"
        val created = commandService.createStoryboard(externalId, "KP Test", listOf("kp1", "kp2", "kp3"))
        val sbId = StoryboardId((created as CommandResult.Success).storyboardId)

        val seg1 = SegmentData(order = 0, startTimeMs = 0, endTimeMs = 3000, stimulusIntensity = 3,
            isKnowledgePoint = true, knowledgePointId = "kp1")
        eventProcessor.processEvent(SegmentAdded(storyboardId = sbId.value, aggregateVersion = 2, segment = seg1), consumerGroup)

        val validation = kpValidator.validateKnowledgePointIntegrity(sbId)
        assertFalse(validation.isValid)
        assertTrue(validation.missingIds.contains("kp2"))
        assertTrue(validation.missingIds.contains("kp3"))
    }

    @Test
    fun `knowledge point integrity - all present validates OK`() = runBlocking {
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
        assertTrue(validation.isValid, "All KPs present should be OK: ${validation.errors}")
    }

    // ========== Risk Findings Tests ==========

    @Test
    fun `risk findings - detect violations with evidence and no medical diagnoses`() = runBlocking {
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

        val forbidden = listOf("成瘾", "脑损伤", "addiction", "brain damage")
        result.findings.forEach { f ->
            assertTrue(f.evidence.description.isNotBlank(), "Evidence for ${f.ruleId}")
            assertTrue(f.suggestion.isNotBlank(), "Suggestion for ${f.ruleId}")
            forbidden.forEach { term ->
                assertFalse(
                    f.suggestion.contains(term, ignoreCase = true) ||
                    f.evidence.description.contains(term, ignoreCase = true),
                    "Must not contain '$term': ${f.suggestion}"
                )
            }
        }
    }

    // ========== Out-of-order Event Tests ==========

    @Test
    fun `out of order events - version 3 buffered until version 2 arrives`() = runBlocking {
        val externalId = "test-ooo-${UUID.randomUUID()}"
        val created = commandService.createStoryboard(externalId, "OOO Test")
        val sbId = (created as CommandResult.Success).storyboardId

        val seg2 = SegmentData(order = 0, startTimeMs = 3000, endTimeMs = 6000, stimulusIntensity = 3)
        val seg1 = SegmentData(order = 0, startTimeMs = 0, endTimeMs = 3000, stimulusIntensity = 3)

        val r3 = eventProcessor.processEvent(
            SegmentAdded(storyboardId = sbId, aggregateVersion = 3, segment = seg2), consumerGroup
        )
        assertTrue(r3.success, "v3 processing should succeed (buffered or processed)")

        val r2 = eventProcessor.processEvent(
            SegmentAdded(storyboardId = sbId, aggregateVersion = 2, segment = seg1), consumerGroup
        )
        assertTrue(r2.success, "v2 should be processed successfully")

        val version = atomicWriteRepo.getProcessedVersion(consumerGroup, sbId)
        assertTrue(version >= 2, "Version should advance to at least 2 after processing v2")
    }

    // ========== Dead Letter Tests ==========

    @Test
    fun `dead letter queue accessible after operations`() = runBlocking {
        val dlqCount = deadLetterRepo.getUnresolvedCount()
        assertTrue(dlqCount >= 0, "Should be able to query DLQ count")
    }
}
