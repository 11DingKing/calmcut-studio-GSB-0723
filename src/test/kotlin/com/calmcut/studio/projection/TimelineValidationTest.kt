package com.calmcut.studio.projection

import com.calmcut.studio.domain.KnowledgePoint
import com.calmcut.studio.domain.KnowledgePointSet
import com.calmcut.studio.domain.Timeline
import com.calmcut.studio.testutil.TestFixtures.seg
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** Verifies the timeline stays continuous/non-overlapping and KP integrity rules. */
class TimelineValidationTest {

    @Test
    fun `start times are cumulative and non overlapping`() {
        val t = Timeline.of(listOf(seg("a", 0, durationMs = 1000), seg("b", 1, durationMs = 2000), seg("c", 2, durationMs = 500)))
        assertEquals(0, t.startMs(0))
        assertEquals(1000, t.startMs(1))
        assertEquals(3000, t.startMs(2))
        assertEquals(3500, t.totalDurationMs)
        // Non-overlap: each segment's end equals the next segment's start.
        for (i in 0 until t.size - 1) {
            assertEquals(t.endMs(i), t.startMs(i + 1))
        }
    }

    @Test
    fun `timeline sorts by order regardless of input order`() {
        val t = Timeline.of(listOf(seg("c", 2), seg("a", 0), seg("b", 1)))
        assertEquals(listOf("a", "b", "c"), t.segments.map { it.id })
    }

    @Test
    fun `duplicate order index is rejected`() {
        assertFailsWith<IllegalArgumentException> {
            Timeline.of(listOf(seg("a", 0), seg("b", 0)))
        }
    }

    @Test
    fun `invalid intensity is rejected`() {
        assertFailsWith<IllegalArgumentException> { seg("a", 0, intensity = 6) }
    }

    @Test
    fun `knowledge point integrity passes when sets align`() {
        val kp = KnowledgePointSet(
            original = listOf(KnowledgePoint("k1", "A"), KnowledgePoint("k2", "B")),
            revision = listOf(KnowledgePoint("k1", "A'"), KnowledgePoint("k2", "B'")),
        )
        assertTrue(KnowledgePointValidator.validate(kp).ok)
    }

    @Test
    fun `knowledge point integrity fails when original dropped in revision`() {
        val kp = KnowledgePointSet(
            original = listOf(KnowledgePoint("k1", "A"), KnowledgePoint("k2", "B")),
            revision = listOf(KnowledgePoint("k1", "A'")),
        )
        val r = KnowledgePointValidator.validate(kp)
        assertFalse(r.ok)
        assertTrue(r.violations.any { it.contains("k2") && it.contains("missing") })
    }

    @Test
    fun `knowledge point integrity fails on orphan revision point`() {
        val kp = KnowledgePointSet(
            original = listOf(KnowledgePoint("k1", "A")),
            revision = listOf(KnowledgePoint("k1", "A'"), KnowledgePoint("k9", "orphan")),
        )
        val r = KnowledgePointValidator.validate(kp)
        assertFalse(r.ok)
        assertTrue(r.violations.any { it.contains("k9") })
    }
}
