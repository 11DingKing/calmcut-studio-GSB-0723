package com.calmcut.studio.api

import com.calmcut.studio.domain.model.*
import kotlinx.serialization.Serializable

@Serializable
data class SegmentDto(
    val id: String,
    val orderIndex: Int,
    val startTimeMs: Long,
    val endTimeMs: Long,
    val intensity: Int,
    val isReversal: Boolean = false,
    val isKnowledgePoint: Boolean = false,
    val isDeclineInducement: Boolean = false,
    val knowledgePointId: String? = null,
    val isOriginal: Boolean = true,
    val content: String? = null
) {
    fun toModel(timelineId: String, version: Long): StoryboardSegment = StoryboardSegment(
        id = id,
        timelineId = timelineId,
        version = version,
        orderIndex = orderIndex,
        startTimeMs = startTimeMs,
        endTimeMs = endTimeMs,
        intensity = intensity,
        isReversal = isReversal,
        isKnowledgePoint = isKnowledgePoint,
        isDeclineInducement = isDeclineInducement,
        knowledgePointId = knowledgePointId,
        isOriginal = isOriginal,
        content = content
    )
}

@Serializable
data class CreateSegmentRequest(
    val segment: SegmentDto,
    val expectedVersion: Long
)

@Serializable
data class UpdateSegmentRequest(
    val segment: SegmentDto,
    val expectedVersion: Long
)

@Serializable
data class DeleteSegmentRequest(
    val expectedVersion: Long
)

@Serializable
data class BatchImportRequest(
    val segments: List<SegmentDto>,
    val mode: ImportMode = ImportMode.REPLACE
) {
    @Serializable
    enum class ImportMode { APPEND, REPLACE }
}

@Serializable
data class TimelineResponse(
    val timelineId: String,
    val version: Long,
    val segments: List<SegmentDto>,
    val totalDurationMs: Long,
    val totalSegments: Int
)

@Serializable
data class RiskFindingDto(
    val findingId: String,
    val ruleId: String,
    val ruleVersion: String,
    val severity: String,
    val segmentIds: List<String>,
    val timeRangeStartMs: Long?,
    val timeRangeEndMs: Long?,
    val evidence: kotlinx.serialization.json.JsonElement,
    val suggestion: String
)

@Serializable
data class AnalysisResponse(
    val timelineId: String,
    val ruleVersion: String,
    val analysisVersion: Long,
    val findings: List<RiskFindingDto>,
    val isIncremental: Boolean,
    val driftCheckPassed: Boolean?
)

@Serializable
data class ImportJobRequest(
    val timelineId: String,
    val totalCount: Long
)

@Serializable
data class ImportChunkRequest(
    val chunkIndex: Int,
    val segments: List<SegmentDto>,
    val isLast: Boolean = false
)

@Serializable
data class ImportJobResponse(
    val jobId: String,
    val timelineId: String,
    val status: String,
    val totalCount: Long,
    val processedCount: Long,
    val failedCount: Long,
    val progress: Double,
    val resumeToken: String? = null,
    val errorMessage: String? = null
)

@Serializable
data class EventAckResponse(
    val eventId: String,
    val timelineId: String,
    val version: Long,
    val accepted: Boolean
)

@Serializable
data class ErrorResponse(
    val error: String,
    val message: String,
    val details: String? = null
)
