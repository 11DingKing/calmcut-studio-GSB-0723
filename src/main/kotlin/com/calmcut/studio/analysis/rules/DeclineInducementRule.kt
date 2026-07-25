package com.calmcut.studio.analysis.rules

import com.calmcut.studio.analysis.AffectedRange
import com.calmcut.studio.analysis.AnalysisSettings
import com.calmcut.studio.analysis.RiskCode
import com.calmcut.studio.analysis.RiskFinding
import com.calmcut.studio.analysis.RiskRule
import com.calmcut.studio.domain.Timeline

/**
 * Flags each segment marked as a "keep scrolling / keep sliding" inducement
 * ("出现继续下滑诱导"). This is a per-segment property, so a finding is anchored
 * at the segment itself and only that segment's window matters.
 */
class DeclineInducementRule(private val settings: AnalysisSettings) : RiskRule {
    override val code = RiskCode.DECLINE_INDUCEMENT
    override val version = settings.ruleSetVersion

    override fun evaluate(timeline: Timeline, window: AffectedRange): List<RiskFinding> {
        val out = ArrayList<RiskFinding>()
        for (i in 0 until timeline.size) {
            if (!window.contains(i)) continue
            val seg = timeline.segments[i]
            if (!seg.declineInducement) continue
            out += RiskFinding(
                code = code,
                ruleVersion = version,
                anchorSegmentId = seg.id,
                hitSegmentIds = listOf(seg.id),
                evidence = "镜头 ${seg.id} 含继续下滑诱导设计。",
                suggestion = "移除或弱化该镜头的继续下滑诱导，改为与知识点相关的自然收尾。",
            )
        }
        return out
    }

    override fun affectedWindow(timeline: Timeline, changed: AffectedRange): AffectedRange = changed
}
