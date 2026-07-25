package com.calmcut.studio.worker

import com.calmcut.studio.analysis.AffectedRange
import com.calmcut.studio.domain.Timeline

/**
 * Computes the minimal changed index range between two consecutive timelines so
 * the analyzer can bound its incremental recomputation. Compares from both ends
 * inward; returns the inclusive index span on the *new* timeline that differs
 * (by id, order, or content). Returns [AffectedRange.ALL] when either timeline is
 * empty or the change cannot be localised safely.
 */
object TimelineDiff {

    fun changedRange(old: Timeline, new: Timeline): AffectedRange {
        val oldSegs = old.segments
        val newSegs = new.segments
        if (oldSegs.isEmpty() || newSegs.isEmpty()) return AffectedRange.ALL

        val minLen = minOf(oldSegs.size, newSegs.size)
        var front = 0
        while (front < minLen && oldSegs[front] == newSegs[front]) front++

        // If everything matched up to the shorter length, the tail is the change.
        if (front == minLen) {
            return AffectedRange(
                (front - 1).coerceAtLeast(0),
                (newSegs.size - 1).coerceAtLeast(0),
            )
        }

        var backOld = oldSegs.size - 1
        var backNew = newSegs.size - 1
        while (backOld >= front && backNew >= front && oldSegs[backOld] == newSegs[backNew]) {
            backOld--
            backNew--
        }
        // Widen by one on each side to include bounding neighbours for context.
        val from = (front - 1).coerceAtLeast(0)
        val to = (backNew + 1).coerceAtMost(newSegs.size - 1)
        return AffectedRange(from, maxOf(from, to))
    }
}
