package com.calmcut.studio.projection

import com.calmcut.studio.domain.EventType
import com.calmcut.studio.domain.KnowledgePointSet
import com.calmcut.studio.domain.Segment
import com.calmcut.studio.domain.StoryboardEvent
import com.calmcut.studio.domain.Timeline

/**
 * In-memory projection of a single storyboard, produced by folding the ordered
 * event stream. This is the deterministic source of truth used to (re)build the
 * persisted projection tables and to detect projection drift.
 */
data class StoryboardState(
    val storyboardId: String,
    val version: Long,
    val segmentsById: Map<String, Segment>,
    val knowledgePoints: KnowledgePointSet,
) {
    fun timeline(): Timeline = Timeline.of(segmentsById.values)

    companion object {
        fun empty(storyboardId: String) =
            StoryboardState(storyboardId, 0, emptyMap(), KnowledgePointSet())
    }
}

/**
 * Pure, deterministic event folding. Applying the same events in the same order
 * always yields the same state — the property that makes "rebuild from zero" and
 * "incremental == full" equivalence possible.
 */
object StoryboardProjector {

    /** Folds a single event onto a prior state. Ignores stale/duplicate versions. */
    fun apply(state: StoryboardState, event: StoryboardEvent): StoryboardState {
        require(event.storyboardId == state.storyboardId) {
            "event storyboard ${event.storyboardId} != state ${state.storyboardId}"
        }
        // Out-of-order / replayed events with non-advancing version are no-ops.
        if (event.version <= state.version) return state

        val segments = state.segmentsById.toMutableMap()
        var kp = state.knowledgePoints

        when (event.type) {
            EventType.BATCH_IMPORT -> {
                segments.clear()
                event.segments.forEach { segments[it.id] = it }
                event.knowledgePoints?.let { kp = it }
            }
            EventType.SEGMENT_CREATED -> {
                event.segments.forEach { segments[it.id] = it }
            }
            EventType.SEGMENT_UPDATED -> {
                event.segments.forEach { segments[it.id] = it }
            }
            EventType.SEGMENT_DELETED -> {
                event.segmentId?.let { segments.remove(it) }
            }
            EventType.KNOWLEDGE_POINTS_SET -> {
                event.knowledgePoints?.let { kp = it }
            }
        }
        return state.copy(version = event.version, segmentsById = segments, knowledgePoints = kp)
    }

    /** Rebuilds full state from an ordered (or unordered) event log, from zero. */
    fun rebuild(storyboardId: String, events: List<StoryboardEvent>): StoryboardState {
        var state = StoryboardState.empty(storyboardId)
        events.sortedBy { it.version }.forEach { state = apply(state, it) }
        return state
    }
}
