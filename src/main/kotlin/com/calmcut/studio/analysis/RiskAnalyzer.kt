package com.calmcut.studio.analysis

import com.calmcut.studio.analysis.rules.AverageDurationRule
import com.calmcut.studio.analysis.rules.ConsecutiveIntensityRule
import com.calmcut.studio.analysis.rules.DeclineInducementRule
import com.calmcut.studio.analysis.rules.KnowledgeBufferRule
import com.calmcut.studio.analysis.rules.ReversalWindowRule
import com.calmcut.studio.domain.Timeline

/**
 * Runs the five risk rules over a storyboard timeline. Supports two modes that
 * are guaranteed to agree finding-for-finding:
 *
 *  - [analyzeFull] re-scans the entire timeline.
 *  - [analyzeIncremental] recomputes only each rule's affected window after a
 *    local edit, reusing prior findings anchored outside that window.
 *
 * Equivalence relies on two invariants:
 *  1. every rule is a pure function of the timeline, and
 *  2. [RiskRule.affectedWindow] fully contains every finding whose membership
 *     could change for a given local edit, so findings anchored outside it are
 *     provably unchanged and safe to carry over.
 */
class RiskAnalyzer(private val settings: AnalysisSettings = AnalysisSettings()) {

    val rules: List<RiskRule> = listOf(
        ReversalWindowRule(settings),
        ConsecutiveIntensityRule(settings),
        AverageDurationRule(settings),
        DeclineInducementRule(settings),
        KnowledgeBufferRule(settings),
    )

    val ruleVersion: String get() = settings.ruleSetVersion

    /** Full re-scan of the whole timeline. */
    fun analyzeFull(storyboardId: String, version: Long, timeline: Timeline): AnalysisResult {
        val findings = rules.flatMap { it.evaluate(timeline, AffectedRange.ALL) }
        return AnalysisResult(storyboardId, version, settings.ruleSetVersion, order(findings))
    }

    /**
     * Recompute only the windows affected by a local edit on [newTimeline]. The
     * [changed] range is expressed in indices on the new timeline (for a delete,
     * pass the index range the removed segment previously occupied, clamped).
     */
    fun analyzeIncremental(
        prior: AnalysisResult,
        version: Long,
        newTimeline: Timeline,
        changed: AffectedRange,
    ): AnalysisResult {
        val priorByCode = prior.findings.groupBy { it.code }
        val merged = ArrayList<RiskFinding>()

        for (rule in rules) {
            val window = rule.affectedWindow(newTimeline, changed)
            // Carry over prior findings for this rule whose anchor still exists on
            // the new timeline and lies strictly outside the recomputed window.
            val kept = (priorByCode[rule.code] ?: emptyList()).filter { f ->
                val idx = if (f.anchorSegmentId.isEmpty()) null else newTimeline.indexOf(f.anchorSegmentId)
                idx != null && !window.contains(idx)
            }
            val recomputed = rule.evaluate(newTimeline, window)
            merged += kept
            merged += recomputed
        }
        return AnalysisResult(prior.storyboardId, version, settings.ruleSetVersion, order(merged))
    }

    /** Deterministic ordering so equal finding sets serialize identically. */
    private fun order(findings: List<RiskFinding>): List<RiskFinding> =
        findings.sortedWith(
            compareBy(
                { it.code.name },
                { it.anchorSegmentId },
                { it.hitSegmentIds.joinToString(",") },
                { it.evidence },
            )
        )
}
