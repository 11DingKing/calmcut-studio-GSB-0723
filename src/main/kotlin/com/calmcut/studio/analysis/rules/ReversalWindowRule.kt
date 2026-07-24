package com.calmcut.studio.analysis.rules

import com.calmcut.studio.analysis.*
import com.calmcut.studio.config.AppConfig
import com.calmcut.studio.domain.model.RiskFinding
import com.calmcut.studio.domain.model.RiskRules
import com.calmcut.studio.domain.model.StoryboardSegment
import kotlinx.serialization.json.*

class ReversalWindowRule : RiskRule {
    override val ruleId = RiskRules.REVERSAL_WINDOW
    override val ruleVersion = "1.0.0"
    override val description = "任意 60 秒窗口内强反转超过 6 次"

    override fun evaluateFull(segments: List<StoryboardSegment>, config: AppConfig.AnalysisConfig): List<RiskFinding> {
        val sorted = segments.sortedByTime()
        val reversals = sorted.filter { it.isReversal }
        if (reversals.size <= config.maxReversalsPerWindow) return emptyList()

        val findings = mutableListOf<RiskFinding>()
        val windowMs = config.windowSizeMs
        var left = 0
        val seenStarts = mutableSetOf<Long>()

        for (right in reversals.indices) {
            while (reversals[right].startTimeMs - reversals[left].startTimeMs >= windowMs) {
                left++
            }
            val count = right - left + 1
            if (count > config.maxReversalsPerWindow) {
                val windowStart = reversals[left].startTimeMs
                val windowEnd = reversals[right].startTimeMs
                if (seenStarts.add(windowStart)) {
                    val windowSegments = reversals.subList(left, right + 1)
                    findings.add(
                        RiskFinding(
                            findingId = newFindingId(),
                            timelineId = sorted.first().timelineId,
                            ruleId = ruleId,
                            ruleVersion = ruleVersion,
                            severity = RiskFinding.Severity.CRITICAL,
                            segmentIds = windowSegments.map { it.id },
                            timeRangeMs = windowStart to windowEnd,
                            evidence = buildJsonObject {
                                put("windowStartMs", windowStart)
                                put("windowEndMs", windowEnd)
                                put("windowSizeMs", windowMs)
                                put("reversalCount", count)
                                put("threshold", config.maxReversalsPerWindow)
                                putJsonArray("reversalTimes") {
                                    windowSegments.forEach { add(it.startTimeMs) }
                                }
                            },
                            suggestion = "60秒窗口内出现${count}次强反转，超过阈值${config.maxReversalsPerWindow}次。建议分散强反转节点，降低节奏感过强的视觉冲击，在反转之间增加过渡内容。",
                            analysisVersion = 0L
                        )
                    )
                }
            }
        }
        return findings
    }

    override fun affectedRange(
        oldSegments: List<StoryboardSegment>,
        newSegments: List<StoryboardSegment>,
        changedSegmentIds: Set<String>,
        config: AppConfig.AnalysisConfig
    ): AffectedRange {
        val changed = newSegments.filter { it.id in changedSegmentIds } +
            oldSegments.filter { it.id in changedSegmentIds && it.id !in newSegments.map { s -> s.id }.toSet() }
        if (changed.isEmpty()) return AffectedRange.none()

        val minTime = changed.minOf { it.startTimeMs }
        val maxTime = changed.maxOf { it.endTimeMs }
        val windowMs = config.windowSizeMs
        return AffectedRange.time((minTime - windowMs)..(maxTime + windowMs))
    }

    override fun evaluateIncremental(
        oldSegments: List<StoryboardSegment>,
        newSegments: List<StoryboardSegment>,
        changedSegmentIds: Set<String>,
        previousFindings: List<RiskFinding>,
        config: AppConfig.AnalysisConfig
    ): List<RiskFinding> {
        val range = affectedRange(oldSegments, newSegments, changedSegmentIds, config)
        val timeRange = range.timeRangeMs ?: return previousFindings

        val newFindings = evaluateFull(newSegments, config)

        val findingsOutsideRange = previousFindings.filter { finding ->
            val tr = finding.timeRangeMs ?: return@filter false
            tr.first < timeRange.first || tr.second > timeRange.last
        }

        val findingsInsideRange = newFindings.filter { finding ->
            val tr = finding.timeRangeMs ?: return@filter true
            tr.first >= timeRange.first && tr.second <= timeRange.last
        }

        return dedupeFindings(findingsOutsideRange + findingsInsideRange)
    }
}
