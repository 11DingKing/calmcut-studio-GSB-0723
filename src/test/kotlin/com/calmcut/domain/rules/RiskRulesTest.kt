package com.calmcut.domain.rules

import com.calmcut.domain.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach

class RiskRulesTest {

    private lateinit var config: RiskRuleConfig
    private lateinit var rules: List<RiskRule>

    @BeforeEach
    fun setup() {
        config = RiskRuleConfig()
        rules = RiskRules.allRules(config)
    }

    @Test
    fun `excessive reversals - should detect more than 6 reversals in 60 second window`() {
        val segments = (0 until 20).map { i ->
            StoryboardSegment(
                id = SegmentId.generate(),
                storyboardId = StoryboardId.generate(),
                order = i,
                startTimeMs = i * 3000L,
                endTimeMs = (i + 1) * 3000L,
                stimulusIntensity = 4,
                hasReversal = i < 7
            )
        }

        val rule = ExcessiveReversalsRule(config)
        val findings = rule.evaluate(segments)

        assertTrue(findings.isNotEmpty(), "Should detect excessive reversals")
        findings.forEach { finding ->
            assertEquals(RuleId.EXCESSIVE_REVERSALS_IN_WINDOW, finding.ruleId)
            assertNotNull(finding.windowStartMs)
            assertNotNull(finding.windowEndMs)
            assertTrue(finding.evidence.metrics["reversalCount"]!! > config.maxReversalsPerWindow)
        }
    }

    @Test
    fun `excessive reversals - should pass when reversals are spread out`() {
        val segments = (0 until 60).map { i ->
            StoryboardSegment(
                id = SegmentId.generate(),
                storyboardId = StoryboardId.generate(),
                order = i,
                startTimeMs = i * 2000L,
                endTimeMs = (i + 1) * 2000L,
                stimulusIntensity = 3,
                hasReversal = i % 10 == 0
            )
        }

        val rule = ExcessiveReversalsRule(config)
        val findings = rule.evaluate(segments)
        assertEquals(0, findings.size, "Should not detect excessive reversals when spread over 120s")
    }

    @Test
    fun `consecutive high stimulus - should detect 3 consecutive intensity 5 segments`() {
        val sid = StoryboardId.generate()
        val segments = (0 until 10).map { i ->
            StoryboardSegment(
                id = SegmentId.generate(),
                storyboardId = sid,
                order = i,
                startTimeMs = i * 3000L,
                endTimeMs = (i + 1) * 3000L,
                stimulusIntensity = if (i in 2..4) 5 else 2
            )
        }

        val rule = ConsecutiveHighStimulusRule(config)
        val findings = rule.evaluate(segments)

        assertEquals(1, findings.size, "Should detect exactly one run of 3 consecutive high stimulus")
        assertEquals(3, findings[0].affectedSegmentIds.size)
        assertEquals(RiskSeverity.MEDIUM, findings[0].severity)
    }

    @Test
    fun `consecutive high stimulus - should detect longer runs with HIGH severity`() {
        val sid = StoryboardId.generate()
        val segments = (0 until 10).map { i ->
            StoryboardSegment(
                id = SegmentId.generate(),
                storyboardId = sid,
                order = i,
                startTimeMs = i * 3000L,
                endTimeMs = (i + 1) * 3000L,
                stimulusIntensity = if (i in 0..4) 5 else 2
            )
        }

        val rule = ConsecutiveHighStimulusRule(config)
        val findings = rule.evaluate(segments)

        assertEquals(1, findings.size)
        assertEquals(5, findings[0].affectedSegmentIds.size)
        assertEquals(RiskSeverity.HIGH, findings[0].severity)
    }

    @Test
    fun `short shot duration - should detect average shot shorter than 3 seconds`() {
        val sid = StoryboardId.generate()
        val segments = (0 until 30).map { i ->
            StoryboardSegment(
                id = SegmentId.generate(),
                storyboardId = sid,
                order = i,
                startTimeMs = i * 1500L,
                endTimeMs = (i + 1) * 1500L,
                stimulusIntensity = 3
            )
        }

        val rule = ShortShotDurationRule(config)
        val findings = rule.evaluate(segments)

        assertTrue(findings.isNotEmpty(), "Should detect short average shot duration")
        findings.forEach { finding ->
            assertEquals(RuleId.SHOT_DURATION_TOO_SHORT, finding.ruleId)
            assertTrue(finding.evidence.metrics["averageShotSeconds"]!! < config.minAverageShotSeconds)
        }
    }

    @Test
    fun `short shot duration - should pass when shots are long enough`() {
        val sid = StoryboardId.generate()
        val segments = (0 until 20).map { i ->
            StoryboardSegment(
                id = SegmentId.generate(),
                storyboardId = sid,
                order = i,
                startTimeMs = i * 4000L,
                endTimeMs = (i + 1) * 4000L,
                stimulusIntensity = 3
            )
        }

        val rule = ShortShotDurationRule(config)
        val findings = rule.evaluate(segments)
        assertEquals(0, findings.size, "Should not detect short shots when average is 4s")
    }

    @Test
    fun `scroll inducement - should detect segments with scroll inducement flag`() {
        val sid = StoryboardId.generate()
        val segments = (0 until 10).map { i ->
            StoryboardSegment(
                id = SegmentId.generate(),
                storyboardId = sid,
                order = i,
                startTimeMs = i * 3000L,
                endTimeMs = (i + 1) * 3000L,
                stimulusIntensity = 3,
                hasScrollInducement = i == 5
            )
        }

        val rule = ScrollInducementRule(config)
        val findings = rule.evaluate(segments)

        assertEquals(1, findings.size)
        assertEquals(RuleId.SCROLL_INDUCEMENT_PRESENT, findings[0].ruleId)
        assertEquals(RiskSeverity.LOW, findings[0].severity)
    }

    @Test
    fun `missing buffer - should detect consecutive knowledge points without buffer`() {
        val sid = StoryboardId.generate()
        val segments = listOf(
            makeSegment(sid, 0, 0L, 5000L, intensity = 3, isKp = true, kpId = "kp1"),
            makeSegment(sid, 1, 5000L, 10000L, intensity = 5, isKp = true, kpId = "kp2")
        )

        val rule = MissingBufferRule(config)
        val findings = rule.evaluate(segments)

        assertEquals(1, findings.size)
        assertEquals(RuleId.MISSING_BUFFER_BETWEEN_KNOWLEDGE_POINTS, findings[0].ruleId)
    }

    @Test
    fun `missing buffer - should pass when low-stimulus buffer exists between knowledge points`() {
        val sid = StoryboardId.generate()
        val segments = listOf(
            makeSegment(sid, 0, 0L, 5000L, intensity = 3, isKp = true, kpId = "kp1"),
            makeSegment(sid, 1, 5000L, 7000L, intensity = 1),
            makeSegment(sid, 2, 7000L, 12000L, intensity = 3, isKp = true, kpId = "kp2")
        )

        val rule = MissingBufferRule(config)
        val findings = rule.evaluate(segments)
        assertEquals(0, findings.size, "Should pass when buffer segment exists")
    }

    @Test
    fun `missing buffer - should fail when buffer between KPs has high intensity`() {
        val sid = StoryboardId.generate()
        val segments = listOf(
            makeSegment(sid, 0, 0L, 5000L, intensity = 3, isKp = true, kpId = "kp1"),
            makeSegment(sid, 1, 5000L, 7000L, intensity = 5),
            makeSegment(sid, 2, 7000L, 12000L, intensity = 3, isKp = true, kpId = "kp2")
        )

        val rule = MissingBufferRule(config)
        val findings = rule.evaluate(segments)
        assertEquals(1, findings.size, "Should detect missing low-stimulus buffer")
    }

    @Test
    fun `empty segments - should produce no findings`() {
        rules.forEach { rule ->
            val findings = rule.evaluate(emptyList())
            assertEquals(0, findings.size, "${rule.ruleId} should return no findings for empty input")
        }
    }

    @Test
    fun `all rules on safe content - should produce minimal findings`() {
        val sid = StoryboardId.generate()
        val segments = (0 until 40).map { i ->
            StoryboardSegment(
                id = SegmentId.generate(),
                storyboardId = sid,
                order = i,
                startTimeMs = i * 4000L,
                endTimeMs = (i + 1) * 4000L,
                stimulusIntensity = 2,
                hasReversal = false,
                isKnowledgePoint = i % 8 == 0,
                knowledgePointId = if (i % 8 == 0) "kp_${i / 8}" else null,
                hasScrollInducement = false
            )
        }

        val allFindings = rules.flatMap { it.evaluate(segments) }
        assertEquals(0, allFindings.size, "Safe content should have no risk findings")
    }

    @Test
    fun `no diagnosis of addiction or brain damage - findings only contain structural content suggestions`() {
        val sid = StoryboardId.generate()
        val segments = (0 until 30).map { i ->
            StoryboardSegment(
                id = SegmentId.generate(),
                storyboardId = sid,
                order = i,
                startTimeMs = i * 1000L,
                endTimeMs = (i + 1) * 1000L,
                stimulusIntensity = 5,
                hasReversal = true,
                hasScrollInducement = true
            )
        }

        val allFindings = rules.flatMap { it.evaluate(segments) }
        allFindings.forEach { finding ->
            assertFalse(
                finding.suggestion.contains("成瘾") || finding.suggestion.contains("脑损伤") ||
                finding.evidence.description.contains("成瘾") || finding.evidence.description.contains("脑损伤"),
                "Findings must not diagnose addiction or predict brain damage"
            )
        }
    }

    private fun makeSegment(
        storyboardId: StoryboardId,
        order: Int,
        startMs: Long,
        endMs: Long,
        intensity: Int,
        isKp: Boolean = false,
        kpId: String? = null,
        hasReversal: Boolean = false
    ): StoryboardSegment = StoryboardSegment(
        id = SegmentId.generate(),
        storyboardId = storyboardId,
        order = order,
        startTimeMs = startMs,
        endTimeMs = endMs,
        stimulusIntensity = intensity,
        hasReversal = hasReversal,
        isKnowledgePoint = isKp,
        knowledgePointId = kpId
    )
}
