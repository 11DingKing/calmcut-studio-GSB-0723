package com.calmcut.domain

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/**
 * Pure logic tests for timeline validation (overlaps and gaps).
 * Tests the algorithm independently of database access.
 */
class TimelineValidationTest {

    private fun validateRanges(ranges: List<Pair<Long, Long>>): TimelineCheckResult {
        val errors = mutableListOf<String>()
        if (ranges.isEmpty()) {
            return TimelineCheckResult(hasOverlaps = false, hasGaps = false, errors = emptyList())
        }
        val sorted = ranges.sortedBy { it.first }
        val overlaps = mutableListOf<String>()
        val gaps = mutableListOf<String>()

        for (i in sorted.indices) {
            val cur = sorted[i]
            if (cur.first >= cur.second) errors.add("Invalid range [${cur.first},${cur.second})")
            if (cur.first < 0) errors.add("Negative start time: ${cur.first}")
        }

        for (i in 0 until sorted.size - 1) {
            val cur = sorted[i]
            val nxt = sorted[i + 1]
            if (nxt.first < cur.second) {
                overlaps.add("Overlap: [${cur.first},${cur.second}) vs [${nxt.first},${nxt.second})")
            } else if (nxt.first > cur.second) {
                gaps.add("Gap: ${cur.second} to ${nxt.first}")
            }
        }
        return TimelineCheckResult(
            hasOverlaps = overlaps.isNotEmpty(),
            hasGaps = gaps.isNotEmpty(),
            errors = errors + overlaps + gaps
        )
    }

    data class TimelineCheckResult(val hasOverlaps: Boolean, val hasGaps: Boolean, val errors: List<String>) {
        val isContinuous get() = !hasOverlaps && !hasGaps && errors.isEmpty()
    }

    // === Add segment tests ===

    @Test
    fun `add first segment - trivially continuous`() {
        val result = validateRanges(listOf(0L to 3000L))
        assertTrue(result.isContinuous)
    }

    @Test
    fun `add contiguous segment at end - OK`() {
        val existing = listOf(0L to 3000L, 3000L to 6000L)
        val withNew = existing + (6000L to 9000L)
        val result = validateRanges(withNew)
        assertTrue(result.isContinuous, "Adding contiguous segment should be OK: ${result.errors}")
    }

    @Test
    fun `add contiguous segment at beginning - OK`() {
        val existing = listOf(3000L to 6000L)
        val withNew = listOf(0L to 3000L) + existing
        val result = validateRanges(withNew)
        assertTrue(result.isContinuous, "Adding segment at beginning should be OK: ${result.errors}")
    }

    @Test
    fun `add overlapping segment - rejected`() {
        val existing = listOf(0L to 5000L)
        val overlapping = 3000L to 8000L
        val result = validateRanges(existing + overlapping)
        assertTrue(result.hasOverlaps, "Overlapping segment must be rejected")
        assertFalse(result.isContinuous)
    }

    @Test
    fun `add segment leaving gap - rejected`() {
        val existing = listOf(0L to 3000L)
        val withGap = existing + (5000L to 8000L)
        val result = validateRanges(withGap)
        assertTrue(result.hasGaps, "Segment creating gap must be rejected")
        assertFalse(result.isContinuous)
    }

    @Test
    fun `add zero-length segment - rejected`() {
        val result = validateRanges(listOf(0L to 0L))
        assertFalse(result.isContinuous, "Zero-length segment invalid")
    }

    @Test
    fun `add negative time segment - rejected`() {
        val result = validateRanges(listOf(-100L to 3000L))
        assertFalse(result.isContinuous, "Negative start time should not be valid for positive validation")
    }

    // === Update segment tests ===

    @Test
    fun `update middle segment extending end without overlap - OK`() {
        // [0,3000) [3000,6000) [6000,9000) -> update middle to [3000,7000) -> must check neighbors
        val original = listOf(0L to 3000L, 3000L to 6000L, 6000L to 9000L)
        // update middle (index 1) to [3000, 7000): remove old, add new
        val updated = original.filterIndexed { i, _ -> i != 1 } + (3000L to 7000L)
        val result = validateRanges(updated)
        // new end is 7000, next start is 6000 -> overlap!
        assertTrue(result.hasOverlaps, "Extending into next segment creates overlap")
    }

    @Test
    fun `update segment shrinking it creates gap - rejected`() {
        val original = listOf(0L to 3000L, 3000L to 6000L, 6000L to 9000L)
        // update middle (index 1) from [3000,6000) to [4000,5000)
        val updated = original.filterIndexed { i, _ -> i != 1 } + (4000L to 5000L)
        val result = validateRanges(updated)
        assertTrue(result.hasGaps, "Shrinking a middle segment creates gaps on both sides")
    }

    @Test
    fun `update segment keeping perfect continuity - OK`() {
        val original = listOf(0L to 3000L, 3000L to 6000L, 6000L to 9000L)
        // update middle to same range but with different intensity (same start/end)
        val updated = original.filterIndexed { i, _ -> i != 1 } + (3000L to 6000L)
        val result = validateRanges(updated)
        assertTrue(result.isContinuous, "Updating without changing time range is OK: ${result.errors}")
    }

    @Test
    fun `update last segment extending further - OK`() {
        val original = listOf(0L to 3000L, 3000L to 6000L)
        val updated = original.filterIndexed { i, _ -> i != 1 } + (3000L to 8000L)
        val result = validateRanges(updated)
        assertTrue(result.isContinuous, "Extending last segment is OK: ${result.errors}")
    }

    // === Delete segment tests ===

    @Test
    fun `delete middle segment creates gap - rejected`() {
        val original = listOf(0L to 3000L, 3000L to 6000L, 6000L to 9000L)
        val afterDelete = original.filterIndexed { i, _ -> i != 1 }
        val result = validateRanges(afterDelete)
        assertTrue(result.hasGaps, "Deleting middle segment creates gap")
        assertFalse(result.isContinuous)
    }

    @Test
    fun `delete first segment - remaining still contiguous`() {
        val original = listOf(0L to 3000L, 3000L to 6000L, 6000L to 9000L)
        val afterDelete = original.filterIndexed { i, _ -> i != 0 }
        val result = validateRanges(afterDelete)
        assertTrue(result.isContinuous, "Deleting first segment leaves contiguous remainder: ${result.errors}")
    }

    @Test
    fun `delete last segment - remaining still contiguous`() {
        val original = listOf(0L to 3000L, 3000L to 6000L, 6000L to 9000L)
        val afterDelete = original.filterIndexed { i, _ -> i != 2 }
        val result = validateRanges(afterDelete)
        assertTrue(result.isContinuous, "Deleting last segment leaves contiguous remainder: ${result.errors}")
    }

    @Test
    fun `delete only segment - OK`() {
        val result = validateRanges(emptyList())
        assertTrue(result.isContinuous, "Deleting the only segment is fine")
    }

    // === Batch import tests ===

    @Test
    fun `batch import perfectly tiling timeline - OK`() {
        val segments = (0 until 10).map { it * 3000L to (it + 1) * 3000L }
        val result = validateRanges(segments)
        assertTrue(result.isContinuous, "Perfectly tiled batch should pass: ${result.errors}")
    }

    @Test
    fun `batch import with overlapping segments - rejected`() {
        val segments = mutableListOf<Pair<Long, Long>>()
        for (i in 0 until 10) {
            val start = i * 3000L
            segments.add(start to (start + 3000L))
        }
        segments.add(2000L to 5000L)
        val result = validateRanges(segments)
        assertTrue(result.hasOverlaps, "Batch with overlap must be rejected")
    }

    @Test
    fun `batch import with gaps - rejected`() {
        val segments = listOf(
            0L to 3000L,
            5000L to 8000L,
            8000L to 11000L
        )
        val result = validateRanges(segments)
        assertTrue(result.hasGaps, "Batch with gap must be rejected")
    }

    @Test
    fun `batch import unsorted but tiles perfectly - OK`() {
        val segments = listOf(
            6000L to 9000L,
            0L to 3000L,
            3000L to 6000L
        )
        val result = validateRanges(segments)
        assertTrue(result.isContinuous, "Unsorted but tiling should be OK after sorting: ${result.errors}")
    }

    // === Complex scenarios ===

    @Test
    fun `long continuous timeline with 100 segments - OK`() {
        val segments = (0 until 100).map { it * 1000L to (it + 1) * 1000L }
        val result = validateRanges(segments)
        assertTrue(result.isContinuous, "100 contiguous segments should pass")
    }

    @Test
    fun `single overlap buried in long timeline - detected`() {
        val segments = (0 until 50).map { it * 1000L to (it + 1) * 1000L }.toMutableList()
        segments.add(25500L to 26500L)
        val result = validateRanges(segments)
        assertTrue(result.hasOverlaps, "Single overlap in long timeline must be detected")
    }

    @Test
    fun `errors list includes both overlap and gap messages when both exist`() {
        val segments = listOf(
            0L to 5000L,
            3000L to 8000L,
            10000L to 13000L
        )
        val result = validateRanges(segments)
        assertTrue(result.hasOverlaps)
        assertTrue(result.hasGaps)
        assertTrue(result.errors.size >= 2, "Should report both overlap and gap")
    }
}
