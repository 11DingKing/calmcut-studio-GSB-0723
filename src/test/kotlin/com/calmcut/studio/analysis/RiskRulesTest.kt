package com.calmcut.studio.analysis

import com.calmcut.studio.analysis.rules.*
import com.calmcut.studio.domain.model.RiskRules
import com.calmcut.studio.testutil.TestFixtures
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.assertFalse

class RiskRulesTest {

    private val config = TestFixtures.defaultAnalysisConfig()

    @Test
    fun `R001 reversal window - detects more than 6 reversals in 60 seconds`() {
        val rule = ReversalWindowRule()
        val segments = TestFixtures.reversalsWithinWindow(7, windowSeconds = 60)
        val findings = rule.evaluateFull(segments, config)
        assertEquals(1, findings.size)
        assertEquals(RiskRules.REVERSAL_WINDOW, findings[0].ruleId)
        assertEquals(7, (findings[0].evidence as kotlinx.serialization.json.JsonObject)["reversalCount"].toString().toInt())
    }

    @Test
    fun `R001 reversal window - no finding when 6 or fewer reversals`() {
        val rule = ReversalWindowRule()
        val segments = TestFixtures.reversalsWithinWindow(6, windowSeconds = 60)
        val findings = rule.evaluateFull(segments, config)
        assertEquals(0, findings.size)
    }

    @Test
    fun `R001 reversal window - reversals spread over more than 60 seconds are fine`() {
        val rule = ReversalWindowRule()
        val segments = (0 until 10).map { i ->
            TestFixtures.segment(
                id = "rev-$i",
                orderIndex = i,
                startTimeMs = i * 15000L,
                durationMs = 2000L,
                intensity = 4,
                isReversal = true
            )
        }
        val findings = rule.evaluateFull(segments, config)
        assertEquals(0, findings.size)
    }

    @Test
    fun `R002 consecutive intensity - detects 3 consecutive intensity 5 segments`() {
        val rule = ConsecutiveIntensityRule()
        val segments = (0 until 5).map { i ->
            TestFixtures.segment(
                id = "seg-$i",
                orderIndex = i,
                startTimeMs = i * 3000L,
                durationMs = 3000L,
                intensity = if (i in 1..3) 5 else 2
            )
        }
        val findings = rule.evaluateFull(segments, config)
        assertTrue(findings.isNotEmpty())
        assertEquals(RiskRules.CONSECUTIVE_INTENSITY, findings[0].ruleId)
        assertEquals(3, findings[0].segmentIds.size)
    }

    @Test
    fun `R002 consecutive intensity - no finding when max 2 consecutive high intensity`() {
        val rule = ConsecutiveIntensityRule()
        val segments = listOf(
            TestFixtures.segment(id = "s0", orderIndex = 0, intensity = 5),
            TestFixtures.segment(id = "s1", orderIndex = 1, intensity = 5),
            TestFixtures.segment(id = "s2", orderIndex = 2, intensity = 3),
            TestFixtures.segment(id = "s3", orderIndex = 3, intensity = 5),
            TestFixtures.segment(id = "s4", orderIndex = 4, intensity = 5)
        )
        val findings = rule.evaluateFull(segments, config)
        assertEquals(0, findings.size)
    }

    @Test
    fun `R003 average duration - flags when average shorter than 3 seconds`() {
        val rule = AverageDurationRule()
        val segments = (0 until 10).map { i ->
            TestFixtures.segment(
                id = "s$i", orderIndex = i,
                startTimeMs = i * 2000L, durationMs = 2000L,
                intensity = 3
            )
        }
        val findings = rule.evaluateFull(segments, config)
        assertEquals(1, findings.size)
        assertEquals(RiskRules.AVG_DURATION, findings[0].ruleId)
    }

    @Test
    fun `R003 average duration - no finding when average is 3 seconds or more`() {
        val rule = AverageDurationRule()
        val segments = (0 until 10).map { i ->
            TestFixtures.segment(
                id = "s$i", orderIndex = i,
                startTimeMs = i * 3000L, durationMs = 3000L,
                intensity = 3
            )
        }
        val findings = rule.evaluateFull(segments, config)
        assertEquals(0, findings.size)
    }

    @Test
    fun `R004 decline inducement - detects segments with inducement flag`() {
        val rule = DeclineInducementRule()
        val segments = listOf(
            TestFixtures.segment(id = "s0", orderIndex = 0),
            TestFixtures.segment(id = "s1", orderIndex = 1, isDeclineInducement = true),
            TestFixtures.segment(id = "s2", orderIndex = 2)
        )
        val findings = rule.evaluateFull(segments, config)
        assertEquals(1, findings.size)
        assertEquals(RiskRules.DECLINE_INDUCEMENT, findings[0].ruleId)
        assertEquals("s1", findings[0].segmentIds[0])
    }

    @Test
    fun `R005 knowledge buffer - flags adjacent knowledge points without buffer`() {
        val rule = KnowledgeBufferRule()
        val segments = listOf(
            TestFixtures.segment(id = "kp1", orderIndex = 0, isKnowledgePoint = true, intensity = 4),
            TestFixtures.segment(id = "kp2", orderIndex = 1, isKnowledgePoint = true, intensity = 4)
        )
        val findings = rule.evaluateFull(segments, config)
        assertEquals(1, findings.size)
        assertEquals(RiskRules.KNOWLEDGE_BUFFER, findings[0].ruleId)
    }

    @Test
    fun `R005 knowledge buffer - no finding when low-intensity buffer exists`() {
        val rule = KnowledgeBufferRule()
        val segments = listOf(
            TestFixtures.segment(id = "kp1", orderIndex = 0, isKnowledgePoint = true, intensity = 4),
            TestFixtures.segment(id = "buf", orderIndex = 1, intensity = 2),
            TestFixtures.segment(id = "kp2", orderIndex = 2, isKnowledgePoint = true, intensity = 4)
        )
        val findings = rule.evaluateFull(segments, config)
        assertEquals(0, findings.size)
    }

    @Test
    fun `R005 knowledge buffer - flags high-intensity segments between knowledge points`() {
        val rule = KnowledgeBufferRule()
        val segments = listOf(
            TestFixtures.segment(id = "kp1", orderIndex = 0, isKnowledgePoint = true, intensity = 4),
            TestFixtures.segment(id = "hi", orderIndex = 1, intensity = 5),
            TestFixtures.segment(id = "kp2", orderIndex = 2, isKnowledgePoint = true, intensity = 4)
        )
        val findings = rule.evaluateFull(segments, config)
        assertEquals(1, findings.size)
    }

    @Test
    fun `R005 knowledge buffer - single knowledge point is fine`() {
        val rule = KnowledgeBufferRule()
        val segments = listOf(
            TestFixtures.segment(id = "kp1", orderIndex = 0, isKnowledgePoint = true)
        )
        val findings = rule.evaluateFull(segments, config)
        assertEquals(0, findings.size)
    }

    @Test
    fun `empty timeline produces no findings`() {
        val rules = listOf(
            ReversalWindowRule(),
            ConsecutiveIntensityRule(),
            AverageDurationRule(),
            DeclineInducementRule(),
            KnowledgeBufferRule()
        )
        for (rule in rules) {
            val findings = rule.evaluateFull(emptyList(), config)
            assertEquals(0, findings.size, "Rule ${rule.ruleId} should produce no findings for empty timeline")
        }
    }

    @Test
    fun `all rules have correct versions`() {
        assertEquals("1.0.0", ReversalWindowRule().ruleVersion)
        assertEquals("1.0.0", ConsecutiveIntensityRule().ruleVersion)
        assertEquals("1.0.0", AverageDurationRule().ruleVersion)
        assertEquals("1.0.0", DeclineInducementRule().ruleVersion)
        assertEquals("1.0.0", KnowledgeBufferRule().ruleVersion)
    }

    @Test
    fun `R001 multiple overlapping windows produce distinct findings`() {
        val rule = ReversalWindowRule()
        val segments = (0 until 15).map { i ->
            TestFixtures.segment(
                id = "r$i", orderIndex = i,
                startTimeMs = i * 3000L, durationMs = 2000L,
                intensity = 4, isReversal = true
            )
        }
        val findings = rule.evaluateFull(segments, config)
        assertTrue(findings.isNotEmpty(), "Should detect multiple violation windows with 15 reversals over ~45s")
    }
}
