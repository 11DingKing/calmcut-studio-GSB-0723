package com.calmcut.studio.analysis.rules

import com.calmcut.studio.analysis.AffectedRange
import com.calmcut.studio.analysis.AnalysisSettings
import com.calmcut.studio.analysis.RiskCode
import com.calmcut.studio.analysis.RiskFinding
import com.calmcut.studio.analysis.RiskRule
import com.calmcut.studio.analysis.Windows
import com.calmcut.studio.domain.Timeline

/**
 * Flags any 60-second window that contains more than [AnalysisSettings.maxReversalsPerWindow]
 * strong tonal reversals ("任意 60 秒窗口内强反转超过 6 次").
 *
 * A finding is anchored at each strong-reversal segment i whose forward window
 * [start(i), start(i)+60s) contains too many reversals. Because start times are
 * a cumulative sum, a local edit shifts every later segment uniformly and only
 * changes windows that straddle the edit — so only anchors within one window
 * length to the left of the change can be affected.
 */
class ReversalWindowRule(private val settings: AnalysisSettings) : RiskRule {
    override val code = RiskCode.REVERSAL_DENSITY
    override val version = settings.ruleSetVersion

    override fun evaluate(timeline: Timeline, window: AffectedRange): List<RiskFinding> {
        val out = ArrayList<RiskFinding>()
        val n = timeline.size
        for (i in 0 until n) {
            if (!window.contains(i)) continue
            val seg = timeline.segments[i]
            if (!seg.strongReversal) continue
            val windowEnd = timeline.startMs(i) + settings.reversalWindowMs
            val hits = ArrayList<String>()
            var j = i
            while (j < n && timeline.startMs(j) < windowEnd) {
                if (timeline.segments[j].strongReversal) hits += timeline.segments[j].id
                j++
            }
            if (hits.size > settings.maxReversalsPerWindow) {
                out += RiskFinding(
                    code = code,
                    ruleVersion = version,
                    anchorSegmentId = seg.id,
                    hitSegmentIds = hits,
                    evidence = "自镜头 ${seg.id} 起的 ${settings.reversalWindowMs / 1000} 秒窗口内出现 " +
                        "${hits.size} 次强反转，超过阈值 ${settings.maxReversalsPerWindow} 次。",
                    suggestion = "降低该时间段的强反转密度，将部分反转移出该窗口或替换为平稳过渡镜头。",
                )
            }
        }
        return out
    }

    override fun affectedWindow(timeline: Timeline, changed: AffectedRange): AffectedRange =
        Windows.expandLeftByTime(timeline, changed, settings.reversalWindowMs)
}
