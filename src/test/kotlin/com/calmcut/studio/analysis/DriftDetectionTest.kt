package com.calmcut.studio.analysis

import com.calmcut.studio.domain.EventType
import com.calmcut.studio.domain.Segment
import com.calmcut.studio.domain.StoryboardEvent
import com.calmcut.studio.domain.Timeline
import com.calmcut.studio.projection.StoryboardProjector
import com.calmcut.studio.testutil.TestFixtures.seg
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Verifies projection drift detection: a projection rebuilt from the event log
 * must match the live projection, and any divergence is reported. Also confirms
 * "rebuild from zero" reproduces the identical analysis.
 */
class DriftDetectionTest {

    private val analyzer = RiskAnalyzer(AnalysisSettings())

    @Test
    fun `no drift when projection matches rebuild`() {
        val segs = listOf(seg("a", 0), seg("b", 1), seg("c", 2))
        val result = DriftDetector.compareSegments(segs, segs)
        assertFalse(result.drifted)
    }

    @Test
    fun `drift detected when a segment diverges`() {
        val stored = listOf(seg("a", 0, intensity = 3))
        val rebuilt = listOf(seg("a", 0, intensity = 5))
        val result = DriftDetector.compareSegments(stored, rebuilt)
        assertTrue(result.drifted)
        assertTrue(result.details.any { it.contains("differs") })
    }

    @Test
    fun `drift detected when a segment is missing`() {
        val stored = listOf(seg("a", 0), seg("b", 1))
        val rebuilt = listOf(seg("a", 0))
        assertTrue(DriftDetector.compareSegments(stored, rebuilt).drifted)
    }

    @Test
    fun `rebuild from zero reproduces identical analysis`() {
        val events = listOf(
            StoryboardEvent("e1", "sb", 1, EventType.BATCH_IMPORT, segments = listOf(seg("a", 0, intensity = 5), seg("b", 1, intensity = 5))),
            StoryboardEvent("e2", "sb", 2, EventType.SEGMENT_CREATED, segments = listOf(seg("c", 2, intensity = 5))),
            StoryboardEvent("e3", "sb", 3, EventType.SEGMENT_UPDATED, segmentId = "a", segments = listOf(seg("a", 0, intensity = 2))),
        )
        // Live path: fold then analyze.
        val live = StoryboardProjector.rebuild("sb", events)
        val liveAnalysis = analyzer.analyzeFull("sb", live.version, live.timeline())

        // Rebuild path (shuffled event order, projector re-sorts by version).
        val rebuilt = StoryboardProjector.rebuild("sb", events.shuffled())
        val rebuiltAnalysis = analyzer.analyzeFull("sb", rebuilt.version, rebuilt.timeline())

        assertFalse(DriftDetector.compareAnalysis(liveAnalysis, rebuiltAnalysis).drifted)
    }
}
