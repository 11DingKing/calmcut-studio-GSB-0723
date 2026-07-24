package com.calmcut.studio.domain.model

import kotlinx.serialization.Serializable

@Serializable
data class StoryboardSegment(
    val id: String,
    val timelineId: String,
    val version: Long,
    val orderIndex: Int,
    val startTimeMs: Long,
    val endTimeMs: Long,
    val intensity: Int,
    val isReversal: Boolean,
    val isKnowledgePoint: Boolean,
    val isDeclineInducement: Boolean,
    val knowledgePointId: String? = null,
    val isOriginal: Boolean = true,
    val content: String? = null
) {
    val durationMs: Long get() = endTimeMs - startTimeMs

    init {
        require(endTimeMs > startTimeMs) { "endTimeMs must be greater than startTimeMs for segment $id" }
        require(intensity in 1..5) { "intensity must be between 1 and 5 for segment $id" }
        require(durationMs > 0) { "duration must be positive for segment $id" }
    }

    fun copyWithVersion(newVersion: Long): StoryboardSegment = copy(version = newVersion)
}

@Serializable
data class TimelineState(
    val timelineId: String,
    val version: Long,
    val segments: List<StoryboardSegment>,
    val lastEventId: String? = null
) {
    val totalDurationMs: Long get() = segments.maxOfOrNull { it.endTimeMs } ?: 0L
    val segmentCount: Int get() = segments.size

    fun sortedByOrder(): List<StoryboardSegment> = segments.sortedBy { it.orderIndex }

    fun validateContinuity(): List<String> {
        val errors = mutableListOf<String>()
        val sorted = sortedByOrder()
        for (i in 1 until sorted.size) {
            if (sorted[i - 1].endTimeMs != sorted[i].startTimeMs) {
                errors.add(
                    "Gap/overlap between segment ${sorted[i - 1].id} (ends at ${sorted[i - 1].endTimeMs}) " +
                        "and segment ${sorted[i].id} (starts at ${sorted[i].startTimeMs})"
                )
            }
        }
        for (i in 0 until sorted.size) {
            for (j in i + 1 until sorted.size) {
                if (sorted[i].startTimeMs < sorted[j].endTimeMs && sorted[j].startTimeMs < sorted[i].endTimeMs) {
                    errors.add("Overlap between segment ${sorted[i].id} and ${sorted[j].id}")
                }
            }
        }
        return errors
    }
}
