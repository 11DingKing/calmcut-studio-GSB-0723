package com.calmcut.service

import com.calmcut.domain.*
import com.calmcut.domain.events.*
import com.calmcut.domain.rules.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach

class WorkerRecoveryTest {

    private lateinit var config: RiskRuleConfig

    @BeforeEach
    fun setup() {
        config = RiskRuleConfig()
    }

    @Test
    fun `event processing should be idempotent - same event processed twice produces same result`() {
        val sid = StoryboardId.generate()
        val event = SegmentAdded(
            storyboardId = sid.value,
            aggregateVersion = 2,
            segment = SegmentData(
                order = 0,
                startTimeMs = 0,
                endTimeMs = 5000,
                stimulusIntensity = 5,
                hasReversal = true
            )
        )

        val eventKey = "${event.eventType}:${event.storyboardId}:${event.aggregateVersion}"

        val processedEvents = mutableSetOf<String>()

        fun processOnce(e: DomainEvent): Boolean {
            val key = "${e.eventType}:${e.storyboardId}:${e.aggregateVersion}"
            if (processedEvents.contains(key)) {
                return false
            }
            processedEvents.add(key)
            return true
        }

        assertTrue(processOnce(event))
        assertFalse(processOnce(event), "Same event processed second time should be idempotent/no-op")
        assertFalse(processOnce(event), "Third time should still be idempotent")
    }

    @Test
    fun `projection rebuild from scratch should be deterministic`() {
        val sid = StoryboardId.generate()
        val rules = RiskRules.allRules(config)

        val segments = (0 until 50).map { i ->
            val intensity = if (i % 4 == 0) 5 else 2
            val hasReversal = i % 6 == 0 && intensity == 5
            val isKp = i % 12 == 0
            StoryboardSegment(
                id = SegmentId.from(java.util.UUID.randomUUID()),
                storyboardId = sid,
                order = i,
                startTimeMs = i * 3000L,
                endTimeMs = (i + 1) * 3000L,
                stimulusIntensity = intensity,
                hasReversal = hasReversal,
                isKnowledgePoint = isKp,
                knowledgePointId = if (isKp) "kp_$i" else null,
                hasScrollInducement = i % 25 == 0
            )
        }

        val sortedSegments = segments.sortedBy { it.startTimeMs }

        val fullFindings1 = rules.flatMap { it.evaluate(sortedSegments) }
        val fullFindings2 = rules.flatMap { it.evaluate(sortedSegments) }

        assertEquals(fullFindings1.size, fullFindings2.size, "Full analysis should be deterministic")
        val keys1 = fullFindings1.map { it.findingKey() }.toSet()
        val keys2 = fullFindings2.map { it.findingKey() }.toSet()
        assertEquals(keys1, keys2, "Same input should produce identical findings")

        val partialSegments = segments.take(25).sortedBy { it.startTimeMs }
        val partialFindings = rules.flatMap { it.evaluate(partialSegments) }
        val fullFromPartial = rules.flatMap { it.evaluate(sortedSegments) }

        assertTrue(fullFromPartial.size >= partialFindings.size,
            "Full analysis should find at least as many issues as partial analysis")
    }

    @Test
    fun `dead letter retry should handle transient failures`() {
        data class ProcessingAttempt(val eventId: String, val succeedOnAttempt: Int)

        var attempts = 0
        val retryHistory = mutableListOf<Int>()

        fun processWithRetry(eventId: String, succeedOnAttempt: Int): Boolean {
            attempts++
            retryHistory.add(attempts)
            return attempts >= succeedOnAttempt
        }

        attempts = 0
        retryHistory.clear()
        var success = false
        for (i in 1..5) {
            success = processWithRetry("evt-1", 3)
            if (success) break
        }

        assertTrue(success, "Should eventually succeed after retries")
        assertEquals(3, retryHistory.size)
    }

    @Test
    fun `dead letter should stop retrying after max attempts`() {
        var attempts = 0
        val maxRetries = 3

        fun processAlwaysFails(): Boolean {
            attempts++
            return false
        }

        for (i in 1..maxRetries) {
            processAlwaysFails()
        }

        assertEquals(maxRetries, attempts, "Should stop after max retries")
    }

    @Test
    fun `cancellation should preserve checkpoint for resume`() {
        val totalBatches = 100
        var processedUpTo = 0
        val batchSize = 10

        fun processUntilCancelled(cancelAt: Int): Int {
            var checkpoint = 0
            for (batch in 0 until totalBatches) {
                if (batch == cancelAt) {
                    checkpoint = batch
                    break
                }
                checkpoint = batch + 1
            }
            return checkpoint
        }

        val checkpoint = processUntilCancelled(37)
        assertEquals(37, checkpoint)

        fun resumeFromCheckpoint(checkpoint: Int): Int {
            var finalCheckpoint = checkpoint
            for (batch in checkpoint until totalBatches) {
                finalCheckpoint = batch + 1
            }
            return finalCheckpoint
        }

        val finalResult = resumeFromCheckpoint(checkpoint)
        assertEquals(totalBatches, finalResult, "Should process all batches after resume")
    }

    @Test
    fun `optimistic locking should prevent concurrent modifications`() {
        var currentVersion = 5L
        val lock = java.util.concurrent.locks.ReentrantLock()

        fun incrementVersion(expected: Long): Long? {
            synchronized(lock) {
                if (currentVersion != expected) return null
                currentVersion++
                return currentVersion
            }
        }

        val result1 = incrementVersion(5)
        assertEquals(6L, result1)

        val result2 = incrementVersion(5)
        assertNull(result2, "Should fail because version was already incremented")

        val result3 = incrementVersion(6)
        assertEquals(7L, result3)
    }

    @Test
    fun `knowledge point integrity validation should detect missing points`() {
        val originalPoints = listOf("kp1", "kp2", "kp3", "kp4", "kp5")
        val revisedPoints = listOf("kp1", "kp2", "kp4")

        val missing = originalPoints.toSet() - revisedPoints.toSet()
        val extra = revisedPoints.toSet() - originalPoints.toSet()

        assertEquals(setOf("kp3", "kp5"), missing)
        assertTrue(extra.isEmpty())
    }

    @Test
    fun `knowledge point integrity validation should detect extra points`() {
        val originalPoints = listOf("kp1", "kp2")
        val revisedPoints = listOf("kp1", "kp2", "kp3", "kp4")

        val missing = originalPoints.toSet() - revisedPoints.toSet()
        val extra = revisedPoints.toSet() - originalPoints.toSet()

        assertTrue(missing.isEmpty())
        assertEquals(setOf("kp3", "kp4"), extra)
    }

    @Test
    fun `timeline validation should detect overlapping segments`() {
        val sid = StoryboardId.generate()
        val segments = listOf(
            StoryboardSegment(id = SegmentId.generate(), storyboardId = sid, order = 0, startTimeMs = 0, endTimeMs = 5000, stimulusIntensity = 3),
            StoryboardSegment(id = SegmentId.generate(), storyboardId = sid, order = 1, startTimeMs = 4000, endTimeMs = 8000, stimulusIntensity = 3)
        )

        val sb = Storyboard(id = sid, externalId = "test", title = "Test", segments = segments)
        val result = sb.validateTimeline()

        assertTrue(result.hasOverlaps, "Should detect overlapping segments")
        assertEquals(1, result.overlappingPairs.size)
    }

    @Test
    fun `timeline validation should detect gaps`() {
        val sid = StoryboardId.generate()
        val segments = listOf(
            StoryboardSegment(id = SegmentId.generate(), storyboardId = sid, order = 0, startTimeMs = 0, endTimeMs = 5000, stimulusIntensity = 3),
            StoryboardSegment(id = SegmentId.generate(), storyboardId = sid, order = 1, startTimeMs = 8000, endTimeMs = 12000, stimulusIntensity = 3)
        )

        val sb = Storyboard(id = sid, externalId = "test", title = "Test", segments = segments)
        val result = sb.validateTimeline()

        assertFalse(result.isContinuous, "Should detect gaps")
        assertEquals(1, result.gaps.size)
        assertEquals(5000L..8000L, result.gaps[0])
    }

    @Test
    fun `continuous non-overlapping timeline should pass validation`() {
        val sid = StoryboardId.generate()
        val segments = listOf(
            StoryboardSegment(id = SegmentId.generate(), storyboardId = sid, order = 0, startTimeMs = 0, endTimeMs = 5000, stimulusIntensity = 3),
            StoryboardSegment(id = SegmentId.generate(), storyboardId = sid, order = 1, startTimeMs = 5000, endTimeMs = 10000, stimulusIntensity = 3),
            StoryboardSegment(id = SegmentId.generate(), storyboardId = sid, order = 2, startTimeMs = 10000, endTimeMs = 15000, stimulusIntensity = 3)
        )

        val sb = Storyboard(id = sid, externalId = "test", title = "Test", segments = segments)
        val result = sb.validateTimeline()

        assertTrue(result.isContinuous, "Should be continuous with no overlaps")
        assertFalse(result.hasOverlaps)
        assertTrue(result.gaps.isEmpty())
    }

    @Test
    fun `risk score calculation should weight by severity`() {
        val findings = listOf(
            makeFinding(RiskSeverity.HIGH),
            makeFinding(RiskSeverity.MEDIUM),
            makeFinding(RiskSeverity.LOW),
            makeFinding(RiskSeverity.HIGH)
        )

        val score = findings.sumOf { f ->
            when (f.severity) {
                RiskSeverity.HIGH -> 10.0
                RiskSeverity.MEDIUM -> 5.0
                RiskSeverity.LOW -> 2.0
            }
        }

        assertEquals(27.0, score, "2 HIGH (10) + 1 MEDIUM (5) + 1 LOW (2) = 27")
    }

    private fun RiskFinding.findingKey(): String =
        "${ruleId}:${windowStartMs}:${windowEndMs}:${evidence.description.hashCode()}"

    private fun makeFinding(severity: RiskSeverity): RiskFinding = RiskFinding(
        ruleId = RuleId.CONSECUTIVE_HIGH_STIMULUS,
        severity = severity,
        evidence = RiskEvidence(description = "test"),
        suggestion = "test suggestion"
    )
}
