package com.calmcut.studio.analysis

import com.calmcut.studio.domain.model.TimelineState
import com.calmcut.studio.testutil.TestFixtures
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class IncrementalEquivalenceTest {

    private val config = TestFixtures.defaultAnalysisConfig()
    private val analyzer = RiskAnalyzer(config)

    @Test
    fun `incremental equals full when adding a reversal that triggers R001`() {
        val baseSegments = (0 until 5).map { i ->
            TestFixtures.segment(
                id = "s$i", orderIndex = i,
                startTimeMs = i * 10000L, durationMs = 5000L,
                intensity = 4, isReversal = true
            )
        }
        assertEquivalence(baseSegments, "s6") { segments ->
            segments + TestFixtures.segment(
                id = "s6", orderIndex = 6,
                startTimeMs = 55000L, durationMs = 3000L,
                intensity = 4, isReversal = true
            )
        }
    }

    @Test
    fun `incremental equals full when removing a segment`() {
        val baseSegments = TestFixtures.randomSegments(20, seed = 123)
        assertEquivalence(baseSegments, "rand-5") { segments ->
            segments.filter { it.id != "rand-5" }
        }
    }

    @Test
    fun `incremental equals full when changing intensity to 5 for R002`() {
        val baseSegments = listOf(
            TestFixtures.segment(id = "a", orderIndex = 0, intensity = 5),
            TestFixtures.segment(id = "b", orderIndex = 1, intensity = 5),
            TestFixtures.segment(id = "c", orderIndex = 2, intensity = 3),
            TestFixtures.segment(id = "d", orderIndex = 3, intensity = 5)
        )
        assertEquivalence(baseSegments, "c") { segments ->
            segments.map { if (it.id == "c") it.copy(intensity = 5) else it }
        }
    }

    @Test
    fun `incremental equals full when toggling decline inducement`() {
        val baseSegments = TestFixtures.randomSegments(15, seed = 456)
        assertEquivalence(baseSegments, "rand-7") { segments ->
            segments.map { if (it.id == "rand-7") it.copy(isDeclineInducement = true) else it }
        }
    }

    @Test
    fun `incremental equals full when toggling knowledge points`() {
        val baseSegments = (0 until 10).map { i ->
            TestFixtures.segment(
                id = "kp-$i", orderIndex = i,
                startTimeMs = i * 5000L, durationMs = 5000L,
                intensity = if (i % 3 == 0) 2 else 4
            )
        }
        assertEquivalence(baseSegments, "kp-2") { segments ->
            segments.map { if (it.id == "kp-2") it.copy(isKnowledgePoint = true) else it }
        }
    }

    @Test
    fun `incremental equals full across multiple random mutations`() {
        for (seed in listOf(1L, 42L, 99L, 2024L, 31337L)) {
            val baseSegments = TestFixtures.randomSegments(30, seed = seed)
            val changeId = baseSegments[10].id

            assertEquivalence(baseSegments, changeId) { segments ->
                segments.map { seg ->
                    if (seg.id == changeId) {
                        seg.copy(
                            intensity = (seg.intensity % 5) + 1,
                            isReversal = !seg.isReversal,
                            isDeclineInducement = !seg.isDeclineInducement
                        )
                    } else seg
                }
            }
        }
    }

    @Test
    fun `incremental equals full for large timeline with random changes`() {
        for (seed in listOf(7L, 13L, 99L)) {
            val baseSegments = TestFixtures.randomSegments(100, seed = seed)
            val changeIndex = (seed % 90).toInt() + 5
            val changeId = baseSegments[changeIndex].id

            assertEquivalence(baseSegments, changeId) { segments ->
                segments.map { seg ->
                    if (seg.id == changeId) {
                        seg.copy(intensity = ((seg.intensity + 2) % 5) + 1, isReversal = !seg.isReversal)
                    } else seg
                }
            }
        }
    }

    @Test
    fun `incremental equals full when changing multiple segments`() {
        val baseSegments = TestFixtures.randomSegments(25, seed = 777)
        val changedIds = setOf(baseSegments[3].id, baseSegments[12].id, baseSegments[20].id)

        val newSegments = baseSegments.map { seg ->
            if (seg.id in changedIds) seg.copy(intensity = 5, isReversal = true) else seg
        }

        assertEquivalenceForSet(baseSegments, newSegments, changedIds)
    }

    @Test
    fun `incremental equals full for edge case - all reversals`() {
        val baseSegments = (0 until 20).map { i ->
            TestFixtures.segment(
                id = "r$i", orderIndex = i,
                startTimeMs = i * 2000L, durationMs = 1500L,
                intensity = 5, isReversal = true
            )
        }
        assertEquivalence(baseSegments, "r10") { segments ->
            segments.map { if (it.id == "r10") it.copy(isReversal = false) else it }
        }
    }

    @Test
    fun `incremental equals full for edge case - no risks anywhere`() {
        val baseSegments = (0 until 20).map { i ->
            TestFixtures.segment(
                id = "s$i", orderIndex = i,
                startTimeMs = i * 5000L, durationMs = 5000L,
                intensity = 2
            )
        }
        assertEquivalence(baseSegments, "s10") { segments ->
            segments.map { if (it.id == "s10") it.copy(intensity = 1) else it }
        }
    }

    @Test
    fun `drift detector reports no drift when results match`() {
        val segments = TestFixtures.randomSegments(30, seed = 555)
        val state = TimelineState("test", 1, segments)
        val fullResult = analyzer.analyzeFull(state)
        val drift = analyzer.checkDrift(fullResult, fullResult)
        assertFalse(drift.hasDrift)
        assertEquals(0, drift.mismatches.size)
    }

    private fun assertEquivalence(
        oldSegments: List<com.calmcut.studio.domain.model.StoryboardSegment>,
        changedId: String,
        transform: (List<com.calmcut.studio.domain.model.StoryboardSegment>) -> List<com.calmcut.studio.domain.model.StoryboardSegment>
    ) {
        assertEquivalenceForSet(oldSegments, transform(oldSegments), setOf(changedId))
    }

    private fun assertEquivalenceForSet(
        oldSegments: List<com.calmcut.studio.domain.model.StoryboardSegment>,
        newSegments: List<com.calmcut.studio.domain.model.StoryboardSegment>,
        changedIds: Set<String>
    ) {
        val timelineId = "eq-test"
        val oldState = TimelineState(timelineId, oldSegments.maxOfOrNull { it.version } ?: 1, oldSegments)
        val newVersion = (oldState.version) + 1
        val newState = TimelineState(timelineId, newVersion, newSegments)

        val fullResult = analyzer.analyzeFull(newState)

        val previousFullResult = analyzer.analyzeFull(oldState)
        val incrementalResult = analyzer.analyzeIncremental(
            oldState, newState, previousFullResult.findings, changedIds
        )

        val drift = analyzer.checkDrift(incrementalResult, fullResult)

        assertTrue(
            !drift.hasDrift,
            "Incremental/full equivalence FAILED. Mismatches: ${drift.mismatches}. " +
                "Incremental had ${drift.incrementalCount} findings, full had ${drift.fullCount} findings. " +
                "Changed IDs: $changedIds"
        )

        assertEquals(
            fullResult.findings.size,
            incrementalResult.findings.size,
            "Finding count mismatch. Full=${fullResult.findings.map { it.ruleId }}, Inc=${incrementalResult.findings.map { it.ruleId }}"
        )
    }
}
