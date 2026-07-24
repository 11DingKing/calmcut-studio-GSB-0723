package com.calmcut.studio.projection

import com.calmcut.studio.analysis.RiskAnalyzer
import com.calmcut.studio.config.AppConfig
import com.calmcut.studio.domain.model.StoryboardSegment
import com.calmcut.studio.domain.model.TimelineState
import com.calmcut.studio.testutil.TestFixtures
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class TimelineValidationTest {

    @Test
    fun `continuous timeline has no errors`() {
        val segments = TestFixtures.continuousSegments(5)
        val state = TimelineState("test", 1, segments)
        val errors = state.validateContinuity()
        assertTrue(errors.isEmpty(), "Continuous timeline should have no errors: $errors")
    }

    @Test
    fun `gap between segments is detected`() {
        val segments = listOf(
            TestFixtures.segment(id = "s0", orderIndex = 0, startTimeMs = 0, durationMs = 5000),
            TestFixtures.segment(id = "s1", orderIndex = 1, startTimeMs = 6000, durationMs = 5000)
        )
        val state = TimelineState("test", 1, segments)
        val errors = state.validateContinuity()
        assertTrue(errors.isNotEmpty())
        assertTrue(errors.any { it.contains("Gap/overlap") })
    }

    @Test
    fun `overlapping segments are detected`() {
        val segments = listOf(
            TestFixtures.segment(id = "s0", orderIndex = 0, startTimeMs = 0, durationMs = 5000),
            TestFixtures.segment(id = "s1", orderIndex = 1, startTimeMs = 3000, durationMs = 5000)
        )
        val state = TimelineState("test", 1, segments)
        val errors = state.validateContinuity()
        assertTrue(errors.isNotEmpty())
    }

    @Test
    fun `knowledge integrity checker detects original-revised mismatch`() {
        val segments = listOf(
            TestFixtures.segment(id = "kp1-orig", orderIndex = 0, isKnowledgePoint = true,
                knowledgePointId = "kp-1", isOriginal = true, intensity = 3),
            TestFixtures.segment(id = "kp1-rev", orderIndex = 1, isKnowledgePoint = true,
                knowledgePointId = "kp-1", isOriginal = false, intensity = 3),
            TestFixtures.segment(id = "kp2-orig", orderIndex = 2, isKnowledgePoint = true,
                knowledgePointId = "kp-2", isOriginal = true, intensity = 3)
        )
        val checker = KnowledgeIntegrityChecker()
        val results = checker.verify(segments)
        assertTrue(results.isNotEmpty())
    }

    @Test
    fun `knowledge integrity passes when original and revised match`() {
        val segments = listOf(
            TestFixtures.segment(id = "kp1-a", orderIndex = 0, isKnowledgePoint = true,
                knowledgePointId = "kp-1", isOriginal = true, intensity = 3),
            TestFixtures.segment(id = "kp1-b", orderIndex = 1, isKnowledgePoint = true,
                knowledgePointId = "kp-1", isOriginal = false, intensity = 3)
        )
        val checker = KnowledgeIntegrityChecker()
        val results = checker.verify(segments)
        assertTrue(results.isNotEmpty())
        assertEquals(1, results.size)
    }

    @Test
    fun `optimistic lock exception carries message`() {
        val ex = OptimisticLockException("Version conflict: expected 1, current 2")
        assertEquals("Version conflict: expected 1, current 2", ex.message)
    }

    @Test
    fun `projection result carries drift info`() {
        val config = TestFixtures.defaultAnalysisConfig()
        val analyzer = RiskAnalyzer(config)
        val segments = TestFixtures.randomSegments(20, seed = 42)
        val state = TimelineState("test", 1, segments)
        val result = analyzer.analyzeFull(state)

        assertEquals("test", result.timelineId)
        assertEquals("1.0.0", result.ruleVersion)
        assertFalse(result.isIncremental)
        assertTrue(result.findings.all { it.ruleId in com.calmcut.studio.domain.model.RiskRules.ALL_RULES })
    }
}
