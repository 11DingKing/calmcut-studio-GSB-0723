package com.calmcut.studio.analysis.rules

import com.calmcut.studio.analysis.*
import com.calmcut.studio.config.AppConfig
import com.calmcut.studio.domain.model.RiskFinding
import com.calmcut.studio.domain.model.RiskRules
import com.calmcut.studio.domain.model.StoryboardSegment
import kotlinx.serialization.json.*

class AverageDurationRule : RiskRule {
    override val ruleId = RiskRules.AVG_DURATION
    override val ruleVersion = "1.0.0"
    override val description = "平均镜头短于 3 秒"

    override fun evaluateFull(segments: List<StoryboardSegment>, config: AppConfig.AnalysisConfig): List<RiskFinding> {
        if (segments.isEmpty()) return emptyList()
        val sorted = segments.sortedByOrder()
        val totalDurationMs = sorted.sumOf { it.durationMs }
        val count = sorted.size
        val avgMs = totalDurationMs.toDouble() / count.toDouble()

        if (avgMs < config.minAvgShotSeconds * 1000.0) {
            return listOf(
                RiskFinding(
                    findingId = newFindingId(),
                    timelineId = sorted.first().timelineId,
                    ruleId = ruleId,
                    ruleVersion = ruleVersion,
                    severity = RiskFinding.Severity.WARNING,
                    segmentIds = emptyList(),
                    timeRangeMs = 0L to totalDurationMs,
                    evidence = buildJsonObject {
                        put("averageDurationMs", avgMs)
                        put("averageDurationSeconds", avgMs / 1000.0)
                        put("thresholdSeconds", config.minAvgShotSeconds)
                        put("segmentCount", count)
                        put("totalDurationMs", totalDurationMs)
                    },
                    suggestion = "平均镜头时长为${String.format("%.1f", avgMs / 1000.0)}秒，短于${config.minAvgShotSeconds}秒。过短的镜头可能导致信息传递不充分，建议适当增加镜头时长，确保科普内容有足够的展示和理解时间。",
                    analysisVersion = 0L
                )
            )
        }
        return emptyList()
    }

    override fun affectedRange(
        oldSegments: List<StoryboardSegment>,
        newSegments: List<StoryboardSegment>,
        changedSegmentIds: Set<String>,
        config: AppConfig.AnalysisConfig
    ): AffectedRange = AffectedRange.full()

    override fun evaluateIncremental(
        oldSegments: List<StoryboardSegment>,
        newSegments: List<StoryboardSegment>,
        changedSegmentIds: Set<String>,
        previousFindings: List<RiskFinding>,
        config: AppConfig.AnalysisConfig
    ): List<RiskFinding> {
        return evaluateFull(newSegments, config)
    }
}
