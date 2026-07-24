package com.calmcut.domain

import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import java.util.UUID

@Serializable
data class StoryboardId(val value: String) {
    fun toUUID(): UUID = UUID.fromString(value)
    override fun toString(): String = value

    companion object {
        fun generate(): StoryboardId = StoryboardId(UUID.randomUUID().toString())
        fun from(uuid: UUID): StoryboardId = StoryboardId(uuid.toString())
    }
}

@Serializable
data class SegmentId(val value: String) {
    fun toUUID(): UUID = UUID.fromString(value)
    override fun toString(): String = value

    companion object {
        fun generate(): SegmentId = SegmentId(UUID.randomUUID().toString())
        fun from(uuid: UUID): SegmentId = SegmentId(uuid.toString())
    }
}

@Serializable
data class EventId(val value: Long = 0) {
    override fun toString(): String = value.toString()
}

@Serializable
enum class ContentType {
    CONTENT,
    TRANSITION,
    KNOWLEDGE_POINT,
    BUFFER
}

@Serializable
enum class RiskSeverity {
    LOW,
    MEDIUM,
    HIGH
}

@Serializable
enum class RuleId {
    EXCESSIVE_REVERSALS_IN_WINDOW,
    CONSECUTIVE_HIGH_STIMULUS,
    SHOT_DURATION_TOO_SHORT,
    SCROLL_INDUCEMENT_PRESENT,
    MISSING_BUFFER_BETWEEN_KNOWLEDGE_POINTS
}

@Serializable
data class StoryboardSegment(
    val id: SegmentId = SegmentId.generate(),
    val storyboardId: StoryboardId,
    val order: Int,
    val startTimeMs: Long,
    val endTimeMs: Long,
    val stimulusIntensity: Int,
    val hasReversal: Boolean = false,
    val isKnowledgePoint: Boolean = false,
    val knowledgePointId: String? = null,
    val hasScrollInducement: Boolean = false,
    val contentType: ContentType = ContentType.CONTENT,
    val version: Long = 1
) {
    val durationMs: Long get() = endTimeMs - startTimeMs
    val durationSeconds: Double get() = durationMs / 1000.0

    init {
        require(endTimeMs > startTimeMs) { "endTimeMs must be greater than startTimeMs" }
        require(stimulusIntensity in 1..5) { "stimulusIntensity must be between 1 and 5" }
        require(!isKnowledgePoint || knowledgePointId != null) {
            "knowledgePointId is required when isKnowledgePoint is true"
        }
        require(isKnowledgePoint || knowledgePointId == null) {
            "knowledgePointId must be null when isKnowledgePoint is false"
        }
    }
}

@Serializable
data class Storyboard(
    val id: StoryboardId = StoryboardId.generate(),
    val externalId: String,
    val title: String,
    val currentVersion: Long = 0,
    val knowledgePointCount: Int = 0,
    val originalKnowledgePoints: List<String>? = null,
    val segments: List<StoryboardSegment> = emptyList(),
    val createdAt: Long = System.currentTimeMillis(),
    val updatedAt: Long = System.currentTimeMillis()
) {
    val totalDurationMs: Long
        get() = segments.maxOfOrNull { it.endTimeMs } ?: 0L

    fun sortedByTime(): List<StoryboardSegment> =
        segments.sortedWith(compareBy({ it.startTimeMs }, { it.order }))

    fun validateTimeline(): TimelineValidationResult {
        val sorted = sortedByTime()
        val overlaps = mutableListOf<Pair<StoryboardSegment, StoryboardSegment>>()
        val gaps = mutableListOf<LongRange>()

        for (i in 0 until sorted.size - 1) {
            val current = sorted[i]
            val next = sorted[i + 1]
            if (next.startTimeMs < current.endTimeMs) {
                overlaps.add(current to next)
            } else if (next.startTimeMs > current.endTimeMs) {
                gaps.add(current.endTimeMs..next.startTimeMs)
            }
        }

        return TimelineValidationResult(
            hasOverlaps = overlaps.isNotEmpty(),
            overlappingPairs = overlaps,
            gaps = gaps,
            isContinuous = overlaps.isEmpty() && gaps.isEmpty()
        )
    }
}

@Serializable
data class TimelineValidationResult(
    val hasOverlaps: Boolean,
    val overlappingPairs: List<Pair<StoryboardSegment, StoryboardSegment>>,
    val gaps: List<@Contextual LongRange>,
    val isContinuous: Boolean
)

@Serializable
data class RiskFinding(
    val ruleId: RuleId,
    val severity: RiskSeverity,
    val windowStartMs: Long? = null,
    val windowEndMs: Long? = null,
    val affectedSegmentIds: List<SegmentId> = emptyList(),
    val evidence: RiskEvidence,
    val suggestion: String
)

@Serializable
data class RiskEvidence(
    val description: String,
    val metrics: Map<String, Double> = emptyMap(),
    val segmentDetails: List<SegmentEvidence> = emptyList()
)

@Serializable
data class SegmentEvidence(
    val segmentId: String,
    val order: Int,
    val startTimeMs: Long,
    val endTimeMs: Long,
    val relevantFields: Map<String, String> = emptyMap()
)

@Serializable
data class RiskAnalysisResult(
    val storyboardId: StoryboardId,
    val ruleVersion: String,
    val projectionVersion: Long,
    val computedAt: Long,
    val findings: List<RiskFinding>,
    val totalRiskScore: Double,
    val affectedWindowStartMs: Long? = null,
    val affectedWindowEndMs: Long? = null
)

@Serializable
data class KnowledgePointInfo(
    val id: String,
    val segmentOrder: Int,
    val startTimeMs: Long
)
