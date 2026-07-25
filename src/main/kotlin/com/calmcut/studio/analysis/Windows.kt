package com.calmcut.studio.analysis

import com.calmcut.studio.domain.Timeline

/** Shared helpers for computing local recomputation windows. */
internal object Windows {

    /** Clamp an index into the valid range of [timeline]. */
    fun clamp(timeline: Timeline, index: Int): Int =
        index.coerceIn(0, (timeline.size - 1).coerceAtLeast(0))

    /**
     * Expand [changed] leftwards to include every segment whose start time is
     * within [windowMs] before the start of the changed region, and returns the
     * resulting inclusive range (right edge unchanged). Used by time-window rules
     * whose findings can only be affected within one window-length of the change.
     */
    fun expandLeftByTime(timeline: Timeline, changed: AffectedRange, windowMs: Long): AffectedRange {
        if (timeline.size == 0) return changed
        val lo = clamp(timeline, changed.fromIndex)
        val threshold = timeline.startMs(lo) - windowMs
        var left = lo
        while (left - 1 >= 0 && timeline.startMs(left - 1) >= threshold) left--
        return AffectedRange(left, clamp(timeline, changed.toIndex))
    }

    /**
     * Expand [changed] by [reach] indices on both sides — used by rules whose
     * findings span a bounded number of adjacent segments.
     */
    fun expandByIndex(timeline: Timeline, changed: AffectedRange, reach: Int): AffectedRange {
        if (timeline.size == 0) return changed
        val left = (changed.fromIndex - reach).coerceAtLeast(0)
        val right = (changed.toIndex + reach).coerceAtMost(timeline.size - 1)
        return AffectedRange(left, right)
    }

    /**
     * Expand a run of consecutive segments matching [predicate] outward from the
     * changed region, so that a rule detecting maximal runs re-emits the full run
     * anchored at its (possibly far-left) start.
     */
    fun expandRun(
        timeline: Timeline,
        changed: AffectedRange,
        predicate: (Int) -> Boolean,
    ): AffectedRange {
        if (timeline.size == 0) return changed
        var left = clamp(timeline, changed.fromIndex)
        var right = clamp(timeline, changed.toIndex)
        while (left - 1 >= 0 && predicate(left - 1)) left--
        while (right + 1 < timeline.size && predicate(right + 1)) right++
        // Include one bounding neighbour on each side (the segment that breaks the run).
        return AffectedRange((left - 1).coerceAtLeast(0), (right + 1).coerceAtMost(timeline.size - 1))
    }
}
