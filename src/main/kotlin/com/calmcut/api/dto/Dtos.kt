package com.calmcut.api.dto

import com.calmcut.domain.*
import com.calmcut.domain.events.SegmentChangeSet
import com.calmcut.domain.events.SegmentData
import kotlinx.serialization.Serializable

@Serializable
data class CreateStoryboardRequest(
    val externalId: String,
    val title: String,
    val originalKnowledgePoints: List<String>? = null
)

@Serializable
data class CreateStoryboardResponse(
    val storyboardId: String,
    val version: Long,
    val eventId: String
)

@Serializable
data class AddSegmentRequest(
    val expectedVersion: Long,
    val segment: SegmentDataDto
)

@Serializable
data class UpdateSegmentRequest(
    val expectedVersion: Long,
    val segmentId: String,
    val changes: SegmentChangeSetDto
)

@Serializable
data class DeleteSegmentRequest(
    val expectedVersion: Long,
    val segmentId: String
)

@Serializable
data class ReorderSegmentsRequest(
    val expectedVersion: Long,
    val newOrder: List<String>
)

@Serializable
data class BatchImportRequest(
    val expectedVersion: Long,
    val importJobId: String? = null,
    val segments: List<SegmentDataDto>
)

@Serializable
data class StreamingImportRequest(
    val totalSegments: Int
)

@Serializable
data class StreamingImportResponse(
    val jobId: String,
    val status: String
)

@Serializable
data class ImportJobStatusResponse(
    val jobId: String,
    val storyboardId: String,
    val status: String,
    val totalSegments: Int,
    val processedSegments: Int,
    val failedSegments: Int,
    val checkpoint: Int,
    val startedAt: Long?,
    val completedAt: Long?,
    val cancelledAt: Long?,
    val errorMessage: String?
)

@Serializable
data class CommandResponse(
    val success: Boolean,
    val storyboardId: String? = null,
    val version: Long? = null,
    val eventId: String? = null,
    val error: String? = null,
    val validationErrors: List<String>? = null,
    val conflictExpectedVersion: Long? = null,
    val conflictActualVersion: Long? = null
)

@Serializable
data class SegmentDataDto(
    val id: String? = null,
    val order: Int,
    val startTimeMs: Long,
    val endTimeMs: Long,
    val stimulusIntensity: Int,
    val hasReversal: Boolean = false,
    val isKnowledgePoint: Boolean = false,
    val knowledgePointId: String? = null,
    val hasScrollInducement: Boolean = false,
    val contentType: String = "CONTENT"
)

@Serializable
data class SegmentChangeSetDto(
    val order: Int? = null,
    val startTimeMs: Long? = null,
    val endTimeMs: Long? = null,
    val stimulusIntensity: Int? = null,
    val hasReversal: Boolean? = null,
    val isKnowledgePoint: Boolean? = null,
    val knowledgePointId: String? = null,
    val hasScrollInducement: Boolean? = null,
    val contentType: String? = null
)

@Serializable
data class StoryboardResponse(
    val id: String,
    val externalId: String,
    val title: String,
    val currentVersion: Long,
    val knowledgePointCount: Int,
    val totalDurationMs: Long,
    val createdAt: Long,
    val updatedAt: Long
)

@Serializable
data class SegmentResponse(
    val id: String,
    val order: Int,
    val startTimeMs: Long,
    val endTimeMs: Long,
    val durationMs: Long,
    val stimulusIntensity: Int,
    val hasReversal: Boolean,
    val isKnowledgePoint: Boolean,
    val knowledgePointId: String?,
    val hasScrollInducement: Boolean,
    val contentType: String,
    val version: Long
)

@Serializable
data class RiskAnalysisResponse(
    val storyboardId: String,
    val ruleVersion: String,
    val projectionVersion: Long,
    val computedAt: Long,
    val totalRiskScore: Double,
    val findingCount: Int,
    val findings: List<RiskFindingDto>
)

@Serializable
data class RiskFindingDto(
    val ruleId: String,
    val ruleName: String,
    val severity: String,
    val windowStartMs: Long?,
    val windowEndMs: Long?,
    val affectedSegmentIds: List<String>,
    val evidence: EvidenceDto,
    val suggestion: String
)

@Serializable
data class EvidenceDto(
    val description: String,
    val metrics: Map<String, Double>,
    val segmentDetails: List<SegmentEvidenceDto>
)

@Serializable
data class SegmentEvidenceDto(
    val segmentId: String,
    val order: Int,
    val startTimeMs: Long,
    val endTimeMs: Long,
    val relevantFields: Map<String, String>
)

@Serializable
data class KnowledgePointValidationResponse(
    val isValid: Boolean,
    val originalCount: Int? = null,
    val revisedCount: Int,
    val missingIds: List<String>,
    val extraIds: List<String>,
    val errors: List<String>
)

@Serializable
data class DriftCheckResponse(
    val hasDrift: Boolean,
    val message: String
)

@Serializable
data class RebuildRequest(
    val reason: String = "Manual rebuild requested"
)

fun SegmentDataDto.toSegmentData(): SegmentData = SegmentData(
    id = id,
    order = order,
    startTimeMs = startTimeMs,
    endTimeMs = endTimeMs,
    stimulusIntensity = stimulusIntensity,
    hasReversal = hasReversal,
    isKnowledgePoint = isKnowledgePoint,
    knowledgePointId = knowledgePointId,
    hasScrollInducement = hasScrollInducement,
    contentType = contentType
)

fun SegmentChangeSetDto.toChangeSet(): SegmentChangeSet = SegmentChangeSet(
    order = order,
    startTimeMs = startTimeMs,
    endTimeMs = endTimeMs,
    stimulusIntensity = stimulusIntensity,
    hasReversal = hasReversal,
    isKnowledgePoint = isKnowledgePoint,
    knowledgePointId = knowledgePointId,
    hasScrollInducement = hasScrollInducement,
    contentType = contentType
)

fun RiskAnalysisResult.toResponse(): RiskAnalysisResponse = RiskAnalysisResponse(
    storyboardId = storyboardId.value,
    ruleVersion = ruleVersion,
    projectionVersion = projectionVersion,
    computedAt = computedAt,
    totalRiskScore = totalRiskScore,
    findingCount = findings.size,
    findings = findings.map { it.toDto() }
)

fun RiskFinding.toDto(): RiskFindingDto = RiskFindingDto(
    ruleId = ruleId.name,
    ruleName = ruleId.toDisplayName(),
    severity = severity.name,
    windowStartMs = windowStartMs,
    windowEndMs = windowEndMs,
    affectedSegmentIds = affectedSegmentIds.map { it.value },
    evidence = EvidenceDto(
        description = evidence.description,
        metrics = evidence.metrics,
        segmentDetails = evidence.segmentDetails.map { seg ->
            SegmentEvidenceDto(
                segmentId = seg.segmentId,
                order = seg.order,
                startTimeMs = seg.startTimeMs,
                endTimeMs = seg.endTimeMs,
                relevantFields = seg.relevantFields
            )
        }
    ),
    suggestion = suggestion
)

fun RuleId.toDisplayName(): String = when (this) {
    RuleId.EXCESSIVE_REVERSALS_IN_WINDOW -> "窗口内强反转过多"
    RuleId.CONSECUTIVE_HIGH_STIMULUS -> "连续高刺激片段"
    RuleId.SHOT_DURATION_TOO_SHORT -> "平均镜头时长过短"
    RuleId.SCROLL_INDUCEMENT_PRESENT -> "存在继续下滑诱导"
    RuleId.MISSING_BUFFER_BETWEEN_KNOWLEDGE_POINTS -> "知识点间缺少缓冲段"
}

fun Storyboard.toResponse(): StoryboardResponse = StoryboardResponse(
    id = id.value,
    externalId = externalId,
    title = title,
    currentVersion = currentVersion,
    knowledgePointCount = knowledgePointCount,
    totalDurationMs = totalDurationMs,
    createdAt = createdAt,
    updatedAt = updatedAt
)

fun StoryboardSegment.toResponse(): SegmentResponse = SegmentResponse(
    id = id.value,
    order = order,
    startTimeMs = startTimeMs,
    endTimeMs = endTimeMs,
    durationMs = durationMs,
    stimulusIntensity = stimulusIntensity,
    hasReversal = hasReversal,
    isKnowledgePoint = isKnowledgePoint,
    knowledgePointId = knowledgePointId,
    hasScrollInducement = hasScrollInducement,
    contentType = contentType.name,
    version = version
)
