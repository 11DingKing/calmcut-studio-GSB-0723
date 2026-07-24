package com.calmcut.studio.analysis.rules

import com.calmcut.studio.analysis.AffectedRange
import com.calmcut.studio.analysis.AnalysisSettings
import com.calmcut.studio.analysis.RiskCode
import com.calmcut.studio.analysis.RiskFinding
import com.calmcut.studio.analysis.RiskRule
import com.calmcut.studio.domain.Timeline

/**
 * Flags a storyboard whose mean shot length is shorter than
 * [AnalysisSettings.minAverageShotMs] ("平均镜头短于 3 秒").
 *
 * This is a timeline-global metric: it depends on total duration and segment
 * count, so any edit can change it. Its [affectedWindow] is therefore [ALL] and
 * the analyzer always re-evaluates it in full — cheap, since it is O(1) over the
 * maintained totals. The single finding is anchored globally (empty anchor id).
 */
class AverageDurationRule(private val settings: AnalysisSettings) : RiskRule {
    override val code = RiskCode.SHORT_AVERAGE_SHOT
    override val version = settings.ruleSetVersion

    override fun evaluate(timeline: Timeline, window: AffectedRange): List<RiskFinding> {
        if (timeline.size == 0) return emptyList()
        // Global finding: only emit during a full evaluation (window == ALL) or
        // when index 0 is in the window, to avoid duplicate emission on merges.
        if (!window.contains(0)) return emptyList()
        val avg = timeline.averageDurationMs()
        if (avg >= settings.minAverageShotMs) return emptyList()
        return listOf(
            RiskFinding(
                code = code,
                ruleVersion = version,
                anchorSegmentId = "",
                hitSegmentIds = timeline.segments.map { it.id },
                evidence = "共 ${timeline.size} 段镜头，平均时长 ${"%.2f".format(avg / 1000.0)} 秒，" +
                    "短于阈值 ${settings.minAverageShotMs / 1000.0} 秒。",
                suggestion = "延长部分镜头时长或合并过短镜头，使平均镜头时长不低于阈值。",
            )
        )
    }

    override fun affectedWindow(timeline: Timeline, changed: AffectedRange): AffectedRange =
        AffectedRange.ALL
}
