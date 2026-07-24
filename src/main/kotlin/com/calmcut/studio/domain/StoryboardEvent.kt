package com.calmcut.studio.domain

import kotlinx.serialization.Serializable

/** Types of mutation applied to a storyboard timeline. */
@Serializable
enum class EventType {
    BATCH_IMPORT,
    SEGMENT_CREATED,
    SEGMENT_UPDATED,
    SEGMENT_DELETED,
    KNOWLEDGE_POINTS_SET,
}

/**
 * A knowledge point present in the original and/or revised storyboard. The
 * service validates that no original knowledge point silently disappears and
 * that revision points reference a known original ("原版/修订版知识点完整性校验").
 */
@Serializable
data class KnowledgePoint(
    val id: String,
    val title: String,
)

/** Original vs revised knowledge-point sets carried on a storyboard head. */
@Serializable
data class KnowledgePointSet(
    val original: List<KnowledgePoint> = emptyList(),
    val revision: List<KnowledgePoint> = emptyList(),
)

/**
 * A versioned domain event. [version] is a per-storyboard monotonic counter used
 * for optimistic locking on writes and ordered/idempotent replay on the worker.
 */
@Serializable
data class StoryboardEvent(
    val eventId: String,
    val storyboardId: String,
    val version: Long,
    val type: EventType,
    // For BATCH_IMPORT: the full segment set. For SEGMENT_CREATED/UPDATED: the segment.
    val segments: List<Segment> = emptyList(),
    // For SEGMENT_DELETED / SEGMENT_UPDATED: target segment id.
    val segmentId: String? = null,
    val knowledgePoints: KnowledgePointSet? = null,
)
