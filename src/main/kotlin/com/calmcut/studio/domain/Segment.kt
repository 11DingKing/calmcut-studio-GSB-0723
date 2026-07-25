package com.calmcut.studio.domain

import kotlinx.serialization.Serializable

/**
 * A single storyboard shot ("分镜"). The timeline is derived purely from the
 * ordered sequence of segments: start times are the running sum of durations,
 * so the timeline is always continuous and non-overlapping by construction.
 *
 * @param intensity stimulation strength on a 1..5 scale (5 = strongest).
 * @param strongReversal whether this shot is a "强反转" (strong tonal reversal).
 * @param declineInducement whether this shot induces the viewer to keep
 *        scrolling / sliding ("继续下滑诱导").
 * @param knowledgePoint optional knowledge-point label this shot belongs to.
 */
@Serializable
data class Segment(
    val id: String,
    val orderIndex: Int,
    val durationMs: Long,
    val intensity: Int,
    val strongReversal: Boolean = false,
    val declineInducement: Boolean = false,
    val knowledgePoint: String? = null,
) {
    init {
        require(durationMs > 0) { "segment $id duration must be positive, was $durationMs" }
        require(intensity in 1..5) { "segment $id intensity must be in 1..5, was $intensity" }
        require(orderIndex >= 0) { "segment $id orderIndex must be >= 0, was $orderIndex" }
    }
}

/**
 * An immutable, validated view of a storyboard timeline. Segments are stored in
 * ascending order and each segment's absolute start time is the cumulative sum
 * of preceding durations, guaranteeing a gap-free, non-overlapping timeline.
 */
class Timeline private constructor(
    val segments: List<Segment>,
    private val startMsByIndex: LongArray,
    private val indexById: Map<String, Int>,
) {
    val size: Int get() = segments.size

    fun startMs(index: Int): Long = startMsByIndex[index]

    fun endMs(index: Int): Long = startMsByIndex[index] + segments[index].durationMs

    /** Array position of a segment id, or null if it is not on this timeline. */
    fun indexOf(id: String): Int? = indexById[id]

    val totalDurationMs: Long
        get() = if (segments.isEmpty()) 0 else endMs(segments.lastIndex)

    fun averageDurationMs(): Double =
        if (segments.isEmpty()) 0.0 else totalDurationMs.toDouble() / segments.size

    companion object {
        /**
         * Builds a validated timeline from an arbitrary segment collection.
         * Sorts by [Segment.orderIndex], rejects duplicate order indices, and
         * computes contiguous start times.
         */
        fun of(segments: Collection<Segment>): Timeline {
            val sorted = segments.sortedBy { it.orderIndex }
            val seenOrders = HashSet<Int>(sorted.size)
            val seenIds = HashSet<String>(sorted.size)
            val starts = LongArray(sorted.size)
            val indexById = HashMap<String, Int>(sorted.size)
            var cursor = 0L
            sorted.forEachIndexed { i, seg ->
                require(seenOrders.add(seg.orderIndex)) {
                    "duplicate orderIndex ${seg.orderIndex} in timeline"
                }
                require(seenIds.add(seg.id)) {
                    "duplicate segment id ${seg.id} in timeline"
                }
                starts[i] = cursor
                cursor += seg.durationMs
                indexById[seg.id] = i
            }
            return Timeline(sorted, starts, indexById)
        }
    }
}
