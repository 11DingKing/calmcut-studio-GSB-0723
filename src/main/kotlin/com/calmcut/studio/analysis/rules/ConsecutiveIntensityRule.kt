package com.calmcut.studio.analysis.rules

import com.calmcut.studio.analysis.AffectedRange
import com.calmcut.studio.analysis.AnalysisSettings
import com.calmcut.studio.analysis.RiskCode
import com.calmcut.studio.analysis.RiskFinding
import com.calmcut.studio.analysis.RiskRule
import com.calmcut.studio.analysis.Windows
import com.calmcut.studio.domain.Timeline

/**
 * Flags every maximal run of [AnalysisSettings.consecutiveIntensityCount] or more
 * consecutive segments whose intensity equals [AnalysisSettings.consecutiveIntensityValue]
 * ("连续 3 段刺激强度为 5"). Each run is reported once, anchored at its first segment.
 */
class ConsecutiveIntensityRule(private val settings: AnalysisSettings) : RiskRule {
    override val code = RiskCode.CONSECUTIVE_INTENSITY
    override val version = settings.ruleSetVersion

    private fun matches(timeline: Timeline, i: Int): Boolean =
        timeline.segments[i].intensity == settings.consecutiveIntensityValue

    override fun evaluate(timeline: Timeline, window: AffectedRange): List<RiskFinding> {
        val out = ArrayList<RiskFinding>()
        val n = timeline.size
        var i = 0
        while (i < n) {
            if (!matches(timeline, i)) { i++; continue }
            var j = i
            while (j + 1 < n && matches(timeline, j + 1)) j++
            val runLen = j - i + 1
            // Anchor at the run start; only emit if the anchor lies inside the window.
            if (runLen >= settings.consecutiveIntensityCount && window.contains(i)) {
                val ids = (i..j).map { timeline.segments[it].id }
                out += RiskFinding(
                    code = code,
                    ruleVersion = version,
                    anchorSegmentId = timeline.segments[i].id,
                    hitSegmentIds = ids,
                    evidence = "镜头 ${ids.first()} 起连续 $runLen 段刺激强度均为 " +
                        "${settings.consecutiveIntensityValue}，达到或超过阈值 ${settings.consecutiveIntensityCount} 段。",
                    suggestion = "在该连续高强度段之间插入较低强度的过渡镜头，打散强度峰值。",
                )
            }
            i = j + 1
        }
        return out
    }

    override fun affectedWindow(timeline: Timeline, changed: AffectedRange): AffectedRange =
        Windows.expandRun(timeline, changed) { matches(timeline, it) }
}
