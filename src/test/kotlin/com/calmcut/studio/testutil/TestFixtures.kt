package com.calmcut.studio.testutil

import com.calmcut.studio.domain.Segment

/** Deterministic segment builder for tests. */
object TestFixtures {
    fun seg(
        id: String,
        order: Int,
        durationMs: Long = 4000,
        intensity: Int = 3,
        reversal: Boolean = false,
        decline: Boolean = false,
        kp: String? = null,
    ) = Segment(id, order, durationMs, intensity, reversal, decline, kp)

    fun timelineOf(vararg segs: Segment): List<Segment> = segs.toList()
}
