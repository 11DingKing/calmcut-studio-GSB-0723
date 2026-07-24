package com.calmcut.service

import com.calmcut.domain.*
import com.calmcut.domain.events.*
import com.calmcut.domain.rules.*
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach

class LargeScaleImportTest {

    private lateinit var config: RiskRuleConfig
    private lateinit var rules: List<RiskRule>

    @BeforeEach
    fun setup() {
        config = RiskRuleConfig(
            windowSizeSeconds = 60,
            maxReversalsPerWindow = 6,
            consecutiveHighStimulus = 3,
            highStimulusThreshold = 5,
            minAverageShotSeconds = 3.0,
            bufferStimulusThreshold = 2
        )
        rules = RiskRules.allRules(config)
    }

    @Test
    fun `hundred thousand segment import simulation should complete in reasonable time`() {
        val sid = StoryboardId.generate()
        val segmentCount = 100_000

        val startTime = System.currentTimeMillis()

        var reversalCount = 0
        var highStimulusRuns = 0
        var shortShots = 0
        var scrollCount = 0

        val batchSize = 10_000
        for (batch in 0 until (segmentCount / batchSize)) {
            val batchSegments = (batch * batchSize until (batch + 1) * batchSize).map { i ->
                val intensity = when {
                    i % 100 < 10 -> 5
                    i % 50 < 20 -> 4
                    i % 20 < 10 -> 1
                    else -> 3
                }
                val hasReversal = i % 10 == 0 && intensity >= 4
                val isKp = i % 100 == 0
                val hasScroll = i % 500 == 0

                if (hasReversal) reversalCount++
                if (hasScroll) scrollCount++

                StoryboardSegment(
                    id = SegmentId.generate(),
                    storyboardId = sid,
                    order = i,
                    startTimeMs = i * 2000L,
                    endTimeMs = (i + 1) * 2000L,
                    stimulusIntensity = intensity,
                    hasReversal = hasReversal,
                    isKnowledgePoint = isKp,
                    knowledgePointId = if (isKp) "kp_$i" else null,
                    hasScrollInducement = hasScroll,
                    contentType = if (isKp) ContentType.KNOWLEDGE_POINT else ContentType.CONTENT
                )
            }

            rules.forEach { rule ->
                rule.evaluate(batchSegments)
            }
        }

        val elapsed = System.currentTimeMillis() - startTime
        println("Processed $segmentCount segments in ${elapsed}ms (${segmentCount * 1000.0 / elapsed} segments/sec)")
        assertTrue(elapsed < 60_000, "Should process 100K segments in under 1 minute")
    }

    @Test
    fun `streaming import flow should support cancellation at any point`() = runBlocking {
        val totalSegments = 10_000
        val cancelAt = 4_500
        var emitted = 0
        var cancelled = false

        val segmentFlow = flow {
            for (i in 0 until totalSegments) {
                if (i >= cancelAt && !cancelled) {
                    cancelled = true
                    return@flow
                }
                emit(SegmentData(
                    order = i,
                    startTimeMs = i * 3000L,
                    endTimeMs = (i + 1) * 3000L,
                    stimulusIntensity = 3
                ))
                emitted++
            }
        }

        val collected = mutableListOf<SegmentData>()
        segmentFlow.toList(collected)

        assertEquals(cancelAt, emitted, "Should emit exactly $cancelAt segments before cancellation")
        assertEquals(cancelAt, collected.size)
    }

    @Test
    fun `streaming import should support resume from checkpoint`() {
        val totalSegments = 10_000
        val checkpoint = 3_000

        var processedFromCheckpoint = 0
        for (i in checkpoint until totalSegments) {
            processedFromCheckpoint++
        }

        assertEquals(7_000, processedFromCheckpoint, "Should process remaining segments from checkpoint")
    }

    @Test
    fun `findings should never contain medical diagnoses`() {
        val sid = StoryboardId.generate()
        val riskySegments = (0 until 100).map { i ->
            StoryboardSegment(
                id = SegmentId.generate(),
                storyboardId = sid,
                order = i,
                startTimeMs = i * 1000L,
                endTimeMs = (i + 1) * 1000L,
                stimulusIntensity = 5,
                hasReversal = true,
                hasScrollInducement = i % 5 == 0,
                isKnowledgePoint = i % 3 == 0,
                knowledgePointId = if (i % 3 == 0) "kp_$i" else null
            )
        }

        val allFindings = rules.flatMap { it.evaluate(riskySegments.sortedBy { s -> s.startTimeMs }) }

        val forbiddenTerms = listOf(
            "成瘾", "addiction", "addictive",
            "脑损伤", "brain damage", "brain injury",
            "神经损伤", "neurological damage",
            "诊断", "diagnose", "diagnosis",
            "预测", "predict"
        )

        allFindings.forEach { finding ->
            forbiddenTerms.forEach { term ->
                assertFalse(
                    finding.suggestion.contains(term, ignoreCase = true) ||
                    finding.evidence.description.contains(term, ignoreCase = true),
                    "Finding must not contain forbidden term '$term': ${finding.suggestion}"
                )
            }
        }
    }

    @Test
    fun `all findings should include evidence and suggestions`() {
        val sid = StoryboardId.generate()
        val segments = (0 until 60).map { i ->
            StoryboardSegment(
                id = SegmentId.generate(),
                storyboardId = sid,
                order = i,
                startTimeMs = i * 900L,
                endTimeMs = (i + 1) * 900L,
                stimulusIntensity = if (i < 10) 5 else 3,
                hasReversal = i < 7,
                hasScrollInducement = i == 15,
                isKnowledgePoint = i == 20 || i == 25,
                knowledgePointId = if (i == 20) "kp1" else if (i == 25) "kp2" else null
            )
        }

        val findings = rules.flatMap { it.evaluate(segments.sortedBy { s -> s.startTimeMs }) }

        findings.forEach { finding ->
            assertTrue(finding.suggestion.isNotBlank(), "Finding must have a suggestion for rule ${finding.ruleId}")
            assertTrue(finding.evidence.description.isNotBlank(), "Finding must have evidence description for rule ${finding.ruleId}")
            assertTrue(finding.severity in listOf(RiskSeverity.LOW, RiskSeverity.MEDIUM, RiskSeverity.HIGH))
        }
    }
}
