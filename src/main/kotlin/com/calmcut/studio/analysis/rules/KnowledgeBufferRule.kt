package com.calmcut.studio.analysis.rules

import com.calmcut.studio.analysis.AffectedRange
import com.calmcut.studio.analysis.AnalysisSettings
import com.calmcut.studio.analysis.RiskCode
import com.calmcut.studio.analysis.RiskFinding
import com.calmcut.studio.analysis.RiskRule
import com.calmcut.studio.domain.Timeline

/**
 * Flags a transition between two different knowledge points that is not
 * separated by a low-stimulus buffer segment ("知识点间缺少低刺激缓冲段").
 *
 * A "buffer" is a segment with no knowledge point whose intensity is at or below
 * [AnalysisSettings.lowStimulusIntensity], sitting in the gap between two
 * knowledge-point groups. When two distinct knowledge points meet without such a
 * buffer in between, a finding is anchored at the first segment of the new group.
 */
class KnowledgeBufferRule(private val settings: AnalysisSettings) : RiskRule {
    override val code = RiskCode.MISSING_KNOWLEDGE_BUFFER
    override val version = settings.ruleSetVersion

    override fun evaluate(timeline: Timeline, window: AffectedRange): List<RiskFinding> {
        val out = ArrayList<RiskFinding>()
        val n = timeline.size
        var lastKp: String? = null
        var lastKpSegId: String? = null
        var bufferSeenInGap = false
        for (i in 0 until n) {
            val seg = timeline.segments[i]
            val kp = seg.knowledgePoint
            if (kp == null) {
                if (seg.intensity <= settings.lowStimulusIntensity) bufferSeenInGap = true
                continue
            }
            if (lastKp != null && kp != lastKp && !bufferSeenInGap && window.contains(i)) {
                out += RiskFinding(
                    code = code,
                    ruleVersion = version,
                    anchorSegmentId = seg.id,
                    hitSegmentIds = listOfNotNull(lastKpSegId, seg.id),
                    evidence = "知识点 '$lastKp' 与 '$kp' 之间（镜头 ${seg.id} 处）缺少低刺激缓冲段" +
                        "（强度≤${settings.lowStimulusIntensity} 的无知识点镜头）。",
                    suggestion = "在两个知识点之间插入一段低刺激缓冲镜头，帮助观众消化上一知识点。",
                )
            }
            lastKp = kp
            lastKpSegId = seg.id
            bufferSeenInGap = false
        }
        return out
    }

    /**
     * A change at index k can affect the transition whose gap contains k and the
     * transitions on either side. We widen the range until it has passed two
     * knowledge-point segments to the left and two to the right — a safe
     * over-approximation (over-scanning re-derives identical findings).
     */
    override fun affectedWindow(timeline: Timeline, changed: AffectedRange): AffectedRange {
        val n = timeline.size
        if (n == 0) return changed
        var left = changed.fromIndex.coerceIn(0, n - 1)
        var kpToLeft = 0
        while (left > 0 && kpToLeft < 2) {
            left--
            if (timeline.segments[left].knowledgePoint != null) kpToLeft++
        }
        var right = changed.toIndex.coerceIn(0, n - 1)
        var kpToRight = 0
        while (right < n - 1 && kpToRight < 2) {
            right++
            if (timeline.segments[right].knowledgePoint != null) kpToRight++
        }
        return AffectedRange(left, right)
    }
}
