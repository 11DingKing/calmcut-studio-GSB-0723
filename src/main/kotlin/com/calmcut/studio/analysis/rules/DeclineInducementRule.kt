package com.calmcut.studio.analysis.rules

import com.calmcut.studio.analysis.*
import com.calmcut.studio.config.AppConfig
import com.calmcut.studio.domain.model.RiskFinding
import com.calmcut.studio.domain.model.RiskRules
import com.calmcut.studio.domain.model.StoryboardSegment
import kotlinx.serialization.json.*

class DeclineInducementRule : RiskRule {
    override val ruleId = RiskRules.DECLINE_INDUCEMENT
    override val ruleVersion = "1.0.0"
    override val description = "出现继续下滑诱导"

    override fun evaluateFull(segments: List<StoryboardSegment>, config: AppConfig.AnalysisConfig): List<RiskFinding> {
        val sorted = segments.sortedByOrder()
        return sorted.filter { it.isDeclineInducement }.map { seg ->
            RiskFinding(
                findingId = newFindingId(),
                timelineId = seg.timelineId,
                ruleId = ruleId,
                ruleVersion = ruleVersion,
                severity = RiskFinding.Severity.INFO,
                segmentIds = listOf(seg.id),
                timeRangeMs = seg.startTimeMs to seg.endTimeMs,
                evidence = buildJsonObject {
                    put("segmentId", seg.id)
                    put("orderIndex", seg.orderIndex)
                    put("startTimeMs", seg.startTimeMs)
                    put("endTimeMs", seg.endTimeMs)
                    put("content", seg.content)
                },
                suggestion = "该片段包含继续下滑诱导标记。科普内容应避免诱导用户持续下滑消费，建议在结尾给出总结或思考问题，而非无限滚动引导。",
                analysisVersion = 0L
            )
        }
    }

    override fun affectedRange(
        oldSegments: List<StoryboardSegment>,
        newSegments: List<StoryboardSegment>,
        changedSegmentIds: Set<String>,
        config: AppConfig.AnalysisConfig
    ): AffectedRange {
        if (changedSegmentIds.isEmpty()) return AffectedRange.none()
        val sorted = newSegments.sortedByOrder()
        val indices = changedSegmentIds.mapNotNull { id ->
            sorted.indexOfFirst { it.id == id }.takeIf { it >= 0 }
        }
        if (indices.isEmpty()) {
            val oldSorted = oldSegments.sortedByOrder()
            val oldIdx = changedSegmentIds.mapNotNull { id ->
                oldSorted.indexOfFirst { it.id == id }.takeIf { it >= 0 }
            }
            if (oldIdx.isEmpty()) return AffectedRange.none()
            return AffectedRange.indices(oldIdx.min()..oldIdx.max())
        }
        return AffectedRange.indices(indices.min()..indices.max())
    }

    override fun evaluateIncremental(
        oldSegments: List<StoryboardSegment>,
        newSegments: List<StoryboardSegment>,
        changedSegmentIds: Set<String>,
        previousFindings: List<RiskFinding>,
        config: AppConfig.AnalysisConfig
    ): List<RiskFinding> {
        if (changedSegmentIds.isEmpty()) return previousFindings

        val findingsOutsideRange = previousFindings.filter { f ->
            f.segmentIds.none { it in changedSegmentIds }
        }

        val changedNew = newSegments.filter { it.id in changedSegmentIds && it.isDeclineInducement }
        val newFindingsForChanged = changedNew.map { seg ->
            RiskFinding(
                findingId = newFindingId(),
                timelineId = seg.timelineId,
                ruleId = ruleId,
                ruleVersion = ruleVersion,
                severity = RiskFinding.Severity.INFO,
                segmentIds = listOf(seg.id),
                timeRangeMs = seg.startTimeMs to seg.endTimeMs,
                evidence = buildJsonObject {
                    put("segmentId", seg.id)
                    put("orderIndex", seg.orderIndex)
                    put("startTimeMs", seg.startTimeMs)
                    put("endTimeMs", seg.endTimeMs)
                },
                suggestion = "该片段包含继续下滑诱导标记。科普内容应避免诱导用户持续下滑消费，建议在结尾给出总结或思考问题。",
                analysisVersion = 0L
            )
        }

        return dedupeFindings(findingsOutsideRange + newFindingsForChanged)
    }
}
