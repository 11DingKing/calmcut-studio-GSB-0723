package com.calmcut.studio.analysis

import com.calmcut.studio.domain.Timeline
import kotlinx.serialization.Serializable

/** Stable identifiers for the five explainable risk categories. */
@Serializable
enum class RiskCode {
    REVERSAL_DENSITY,        // 任意 60 秒窗口内强反转超过 6 次
    CONSECUTIVE_INTENSITY,   // 连续 3 段刺激强度为 5
    SHORT_AVERAGE_SHOT,      // 平均镜头短于 3 秒
    DECLINE_INDUCEMENT,      // 出现继续下滑诱导
    MISSING_KNOWLEDGE_BUFFER // 知识点间缺少低刺激缓冲段
}

/**
 * A single explainable finding. Deliberately describes editing-pattern risks
 * only — it never diagnoses addiction or predicts neurological harm; wording is
 * constrained to observable storyboard structure.
 *
 * @param anchorSegmentId stable id of the segment a finding is anchored to. Used
 *        by the analyzer to re-resolve findings against a mutated timeline when
 *        merging incremental results. Empty string for global (timeline-wide)
 *        findings such as short average shot length.
 */
@Serializable
data class RiskFinding(
    val code: RiskCode,
    val ruleVersion: String,
    val anchorSegmentId: String,
    val hitSegmentIds: List<String>,
    val evidence: String,
    val suggestion: String,
)

/** Ordered, deterministic collection of findings for a storyboard version. */
@Serializable
data class AnalysisResult(
    val storyboardId: String,
    val version: Long,
    val ruleVersion: String,
    val findings: List<RiskFinding>,
) {
    /** Comparable canonical form for incremental-vs-full equivalence checks. */
    fun canonical(): List<RiskFinding> =
        findings.map { it.copy() }
            .sortedWith(
                compareBy(
                    { it.code.name },
                    { it.anchorSegmentId },
                    { it.hitSegmentIds.joinToString(",") },
                    { it.evidence },
                )
            )
}

/**
 * Range of segment order-indices affected by a mutation. Rules use this to bound
 * the recomputation window so incremental analysis stays equivalent to full.
 * Ranges are inclusive; [ALL] denotes "the whole timeline".
 */
data class AffectedRange(val fromIndex: Int, val toIndex: Int) {
    fun contains(index: Int): Boolean = index in fromIndex..toIndex

    companion object {
        val ALL = AffectedRange(0, Int.MAX_VALUE)
    }
}

/**
 * A risk rule is a pure function of the timeline. [evaluate] restricted to a
 * window emits only findings anchored within that window (reading neighbours for
 * context as needed); [affectedWindow] widens a locally changed range into the
 * minimal window a rule must re-scan so a partial evaluation is provably
 * identical to a full one. Each emitted finding carries [RiskFinding.anchorSegmentId].
 */
interface RiskRule {
    val code: RiskCode
    val version: String

    /**
     * Emit findings whose anchor index lies within [window]. Passing
     * [AffectedRange.ALL] performs a full-timeline evaluation.
     */
    fun evaluate(timeline: Timeline, window: AffectedRange = AffectedRange.ALL): List<RiskFinding>

    /**
     * Given a locally changed index range on the new timeline, return the widened
     * window that fully contains every finding whose membership could change.
     */
    fun affectedWindow(timeline: Timeline, changed: AffectedRange): AffectedRange
}
