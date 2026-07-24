package com.calmcut.studio.analysis

import com.calmcut.studio.domain.model.RiskAnalysisResult
import com.calmcut.studio.domain.model.RiskFinding
import com.calmcut.studio.domain.model.TimelineState
import com.calmcut.studio.testutil.TestFixtures
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import org.junit.jupiter.api.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import java.util.UUID

class DriftDetectionTest {

    private val config = TestFixtures.defaultAnalysisConfig()
    private val analyzer = RiskAnalyzer(config)

    @Test
    fun `no drift when incremental matches full`() {
        val segments = TestFixtures.randomSegments(30, seed = 100)
        val state = TimelineState("test", 1, segments)
        val result = analyzer.analyzeFull(state)

        val drift = analyzer.checkDrift(result, result)
        assertFalse(drift.hasDrift)
    }

    @Test
    fun `drift detected when finding is missing from incremental`() {
        val segments = TestFixtures.randomSegments(50, seed = 42)
        val state = TimelineState("test", 1, segments)
        val fullResult = analyzer.analyzeFull(state)

        if (fullResult.findings.size < 2) {
            val highIntensitySegments = (0 until 5).map { i ->
                TestFixtures.segment(id = "hi-$i", orderIndex = i, intensity = 5, durationMs = 2000)
            }
            val state2 = TimelineState("test", 1, highIntensitySegments)
            val fullResult2 = analyzer.analyzeFull(state2)
            assertTrue(fullResult2.findings.isNotEmpty(), "Should have at least one finding with intensity 5 segments")
            val inc = fullResult2.copy(findings = fullResult2.findings.drop(1))
            val drift = analyzer.checkDrift(inc, fullResult2)
            assertTrue(drift.hasDrift)
        } else {
            val incrementalResult = fullResult.copy(findings = fullResult.findings.drop(1))
            val drift = analyzer.checkDrift(incrementalResult, fullResult)
            assertTrue(drift.hasDrift)
        }
    }

    @Test
    fun `drift detected when extra finding in incremental`() {
        val segments = (0 until 5).map { i ->
            TestFixtures.segment(id = "hi-$i", orderIndex = i, intensity = 5, durationMs = 2000)
        }
        val state = TimelineState("test", 1, segments)
        val fullResult = analyzer.analyzeFull(state)

        val fakeFinding = RiskFinding(
            findingId = UUID.randomUUID().toString(),
            timelineId = "test",
            ruleId = "R001",
            ruleVersion = "1.0.0",
            severity = RiskFinding.Severity.CRITICAL,
            segmentIds = listOf("fake-seg"),
            timeRangeMs = 0L to 60000L,
            evidence = buildJsonObject { put("fake", JsonPrimitive(true)) },
            suggestion = "Fake finding",
            analysisVersion = 1
        )

        val incrementalResult = fullResult.copy(
            findings = fullResult.findings + fakeFinding
        )

        val drift = analyzer.checkDrift(incrementalResult, fullResult)
        assertTrue(drift.hasDrift)
    }

    @Test
    fun `drift detected when different segments flagged for same rule`() {
        val segments = TestFixtures.randomSegments(30, seed = 200)
        val state = TimelineState("test", 1, segments)
        val fullResult = analyzer.analyzeFull(state)

        if (fullResult.findings.isNotEmpty()) {
            val modifiedFindings = fullResult.findings.toMutableList()
            val firstIdx = modifiedFindings.indexOfFirst { it.ruleId == "R002" }
            if (firstIdx >= 0) {
                modifiedFindings[firstIdx] = modifiedFindings[firstIdx].copy(
                    segmentIds = modifiedFindings[firstIdx].segmentIds + "extra-seg"
                )
            }
            val incrementalResult = fullResult.copy(findings = modifiedFindings)
            val drift = analyzer.checkDrift(incrementalResult, fullResult)
            assertTrue(drift.hasDrift, "Should detect drift when segment sets differ")
        }
    }

    @Test
    fun `empty results match - no drift`() {
        val emptyResult = RiskAnalysisResult(
            timelineId = "test",
            ruleVersion = "1.0.0",
            analysisVersion = 1,
            findings = emptyList(),
            computedFromEventId = null,
            isIncremental = false
        )
        val drift = analyzer.checkDrift(emptyResult, emptyResult)
        assertFalse(drift.hasDrift)
    }

    @Test
    fun `sequence of incremental updates maintains equivalence across many operations`() {
        var segments = TestFixtures.randomSegments(20, seed = 999)
        var version = 1L
        var previousFindings = analyzer.analyzeFull(TimelineState("test", version, segments)).findings

        for (i in 1..20) {
            val changeIdx = (i * 7) % segments.size
            val changeId = segments[changeIdx].id
            val newSegments = segments.map { seg ->
                if (seg.id == changeId) {
                    seg.copy(
                        intensity = ((seg.intensity + i) % 5) + 1,
                        isReversal = i % 3 == 0,
                        isDeclineInducement = i % 5 == 0
                    )
                } else seg
            }

            version++
            val oldState = TimelineState("test", version - 1, segments)
            val newState = TimelineState("test", version, newSegments)

            val fullResult = analyzer.analyzeFull(newState)
            val incrementalResult = analyzer.analyzeIncremental(
                oldState, newState, previousFindings, setOf(changeId)
            )

            val drift = analyzer.checkDrift(incrementalResult, fullResult)
            assertFalse(
                drift.hasDrift,
                "Iteration $i: drift detected! ${drift.mismatches}"
            )

            segments = newSegments
            previousFindings = incrementalResult.findings
        }
    }
}
