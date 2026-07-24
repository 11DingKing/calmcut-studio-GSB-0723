package com.calmcut.studio.analysis

import com.calmcut.studio.analysis.rules.*
import com.calmcut.studio.config.AppConfig
import com.calmcut.studio.domain.model.*

class RiskAnalyzer(private val config: AppConfig.AnalysisConfig) {

    private val rules: List<RiskRule> = listOf(
        ReversalWindowRule(),
        ConsecutiveIntensityRule(),
        AverageDurationRule(),
        DeclineInducementRule(),
        KnowledgeBufferRule()
    )

    fun analyzeFull(timeline: TimelineState): RiskAnalysisResult {
        val findings = mutableListOf<RiskFinding>()
        for (rule in rules) {
            val ruleFindings = rule.evaluateFull(timeline.segments, config)
                .map { it.copy(analysisVersion = timeline.version) }
            findings.addAll(ruleFindings)
        }
        return RiskAnalysisResult(
            timelineId = timeline.timelineId,
            ruleVersion = config.ruleVersion,
            analysisVersion = timeline.version,
            findings = findings,
            computedFromEventId = timeline.lastEventId,
            isIncremental = false
        )
    }

    fun analyzeIncremental(
        oldTimeline: TimelineState,
        newTimeline: TimelineState,
        previousFindings: List<RiskFinding>,
        changedSegmentIds: Set<String>
    ): RiskAnalysisResult {
        val findings = mutableListOf<RiskFinding>()
        for (rule in rules) {
            val previousForRule = previousFindings.filter { it.ruleId == rule.ruleId }
            val ruleFindings = rule.evaluateIncremental(
                oldSegments = oldTimeline.segments,
                newSegments = newTimeline.segments,
                changedSegmentIds = changedSegmentIds,
                previousFindings = previousForRule,
                config = config
            ).map { it.copy(analysisVersion = newTimeline.version) }
            findings.addAll(ruleFindings)
        }
        return RiskAnalysisResult(
            timelineId = newTimeline.timelineId,
            ruleVersion = config.ruleVersion,
            analysisVersion = newTimeline.version,
            findings = dedupeFindings(findings),
            computedFromEventId = newTimeline.lastEventId,
            isIncremental = true
        )
    }

    fun checkDrift(incrementalResult: RiskAnalysisResult, fullResult: RiskAnalysisResult): DriftResult {
        return findingsMatchSet(incrementalResult.findings, fullResult.findings)
    }

    fun getRule(ruleId: String): RiskRule? = rules.find { it.ruleId == ruleId }
}
