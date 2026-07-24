package com.calmcut.studio.api

import com.calmcut.studio.analysis.AnalysisResult
import com.calmcut.studio.analysis.RiskFinding
import com.calmcut.studio.domain.KnowledgePointSet
import com.calmcut.studio.domain.Segment
import kotlinx.serialization.Serializable

@Serializable
data class SegmentDto(
    val id: String,
    val orderIndex: Int,
    val durationMs: Long,
    val intensity: Int,
    val strongReversal: Boolean = false,
    val declineInducement: Boolean = false,
    val knowledgePoint: String? = null,
) {
    fun toDomain() = Segment(id, orderIndex, durationMs, intensity, strongReversal, declineInducement, knowledgePoint)

    companion object {
        fun from(s: Segment) =
            SegmentDto(s.id, s.orderIndex, s.durationMs, s.intensity, s.strongReversal, s.declineInducement, s.knowledgePoint)
    }
}

/** Batch import request. [expectedVersion] enables optimistic-lock rejection. */
@Serializable
data class BatchImportRequest(
    val storyboardId: String,
    val expectedVersion: Long,
    val segments: List<SegmentDto>,
    val knowledgePoints: KnowledgePointSet? = null,
)

@Serializable
data class CreateSegmentRequest(
    val storyboardId: String,
    val expectedVersion: Long,
    val segment: SegmentDto,
)

@Serializable
data class UpdateSegmentRequest(
    val storyboardId: String,
    val expectedVersion: Long,
    val segment: SegmentDto,
)

@Serializable
data class DeleteSegmentRequest(
    val storyboardId: String,
    val expectedVersion: Long,
    val segmentId: String,
)

@Serializable
data class EventAckResponse(
    val eventId: String,
    val storyboardId: String,
    val version: Long,
)

@Serializable
data class RiskFindingDto(
    val code: String,
    val ruleVersion: String,
    val hitSegmentIds: List<String>,
    val evidence: String,
    val suggestion: String,
) {
    companion object {
        fun from(f: RiskFinding) =
            RiskFindingDto(f.code.name, f.ruleVersion, f.hitSegmentIds, f.evidence, f.suggestion)
    }
}

@Serializable
data class AnalysisResponse(
    val storyboardId: String,
    val version: Long,
    val ruleVersion: String,
    val findings: List<RiskFindingDto>,
) {
    companion object {
        fun from(r: AnalysisResult) =
            AnalysisResponse(r.storyboardId, r.version, r.ruleVersion, r.findings.map { RiskFindingDto.from(it) })
    }
}

@Serializable
data class ImportJobResponse(
    val jobId: String,
    val storyboardId: String,
    val status: String,
    val totalSegments: Long,
    val processedSegments: Long,
)

@Serializable
data class ErrorResponse(val error: String, val details: List<String> = emptyList())
