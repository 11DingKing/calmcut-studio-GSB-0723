package com.calmcut.studio.analysis

import com.calmcut.studio.domain.Timeline
import com.calmcut.studio.testutil.TestFixtures.seg
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Verifies each of the five risk rules fires (and does not over-fire) correctly. */
class RiskRulesTest {

    private val analyzer = RiskAnalyzer(AnalysisSettings())

    private fun analyze(segs: List<com.calmcut.studio.domain.Segment>) =
        analyzer.analyzeFull("sb", 1, Timeline.of(segs))

    private fun codes(r: AnalysisResult) = r.findings.map { it.code }.toSet()

    @Test
    fun `reversal density fires when more than six reversals in 60s`() {
        // 8 reversals each 5s long => first 60s window covers indices 0..11 (60s),
        // containing all 8 reversals > 6.
        val segs = (0 until 8).map { seg("r$it", it, durationMs = 5000, reversal = true) }
        val result = analyze(segs)
        assertTrue(RiskCode.REVERSAL_DENSITY in codes(result))
    }

    @Test
    fun `reversal density does not fire at or below threshold`() {
        val segs = (0 until 6).map { seg("r$it", it, durationMs = 5000, reversal = true) }
        val result = analyze(segs)
        assertTrue(RiskCode.REVERSAL_DENSITY !in codes(result))
    }

    @Test
    fun `consecutive intensity fires on three consecutive fives`() {
        val segs = listOf(
            seg("a", 0, intensity = 3),
            seg("b", 1, intensity = 5),
            seg("c", 2, intensity = 5),
            seg("d", 3, intensity = 5),
            seg("e", 4, intensity = 2),
        )
        val result = analyze(segs)
        val finding = result.findings.single { it.code == RiskCode.CONSECUTIVE_INTENSITY }
        assertEquals(listOf("b", "c", "d"), finding.hitSegmentIds)
    }

    @Test
    fun `consecutive intensity does not fire on two fives`() {
        val segs = listOf(
            seg("a", 0, intensity = 5),
            seg("b", 1, intensity = 5),
            seg("c", 2, intensity = 3),
        )
        assertTrue(RiskCode.CONSECUTIVE_INTENSITY !in codes(analyze(segs)))
    }

    @Test
    fun `short average shot fires below three seconds`() {
        val segs = (0 until 5).map { seg("s$it", it, durationMs = 2000) }
        assertTrue(RiskCode.SHORT_AVERAGE_SHOT in codes(analyze(segs)))
    }

    @Test
    fun `short average shot does not fire at three seconds`() {
        val segs = (0 until 5).map { seg("s$it", it, durationMs = 3000) }
        assertTrue(RiskCode.SHORT_AVERAGE_SHOT !in codes(analyze(segs)))
    }

    @Test
    fun `decline inducement fires per marked segment`() {
        val segs = listOf(
            seg("a", 0),
            seg("b", 1, decline = true),
            seg("c", 2, decline = true),
        )
        val findings = analyze(segs).findings.filter { it.code == RiskCode.DECLINE_INDUCEMENT }
        assertEquals(setOf("b", "c"), findings.map { it.anchorSegmentId }.toSet())
    }

    @Test
    fun `missing knowledge buffer fires when two knowledge points are adjacent`() {
        val segs = listOf(
            seg("k1", 0, kp = "KP-A"),
            seg("k2", 1, kp = "KP-B"), // no low-stimulus buffer between A and B
        )
        assertTrue(RiskCode.MISSING_KNOWLEDGE_BUFFER in codes(analyze(segs)))
    }

    @Test
    fun `knowledge buffer satisfied by low stimulus gap`() {
        val segs = listOf(
            seg("k1", 0, kp = "KP-A"),
            seg("buf", 1, intensity = 1), // low-stimulus buffer, no kp
            seg("k2", 2, kp = "KP-B"),
        )
        assertTrue(RiskCode.MISSING_KNOWLEDGE_BUFFER !in codes(analyze(segs)))
    }

    @Test
    fun `findings never contain clinical or addiction language`() {
        val segs = (0 until 8).map {
            seg("r$it", it, durationMs = 2000, intensity = 5, reversal = true, decline = true, kp = "KP-$it")
        }
        val banned = listOf("成瘾", "脑损伤", "addiction", "brain damage", "诊断")
        analyze(segs).findings.forEach { f ->
            val text = (f.evidence + f.suggestion).lowercase()
            banned.forEach { assertTrue(!text.contains(it.lowercase()), "finding leaked banned term '$it'") }
        }
    }
}
