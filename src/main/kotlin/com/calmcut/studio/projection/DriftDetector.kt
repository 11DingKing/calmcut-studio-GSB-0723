package com.calmcut.studio.projection

import com.calmcut.studio.analysis.DriftResult
import com.calmcut.studio.analysis.RiskAnalyzer
import com.calmcut.studio.db.DatabaseFactory
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

class DriftDetector(
    private val repository: ProjectionRepository,
    private val analyzer: RiskAnalyzer
) {
    suspend fun checkTimeline(timelineId: String): DriftResult {
        val state = repository.loadTimeline(timelineId)
            ?: return DriftResult(true, listOf("Timeline $timelineId not found"), 0, 0)

        val storedFindings = repository.loadFindings(timelineId)
        val fullResult = analyzer.analyzeFull(state)

        val drift = analyzer.checkDrift(
            com.calmcut.studio.domain.model.RiskAnalysisResult(
                timelineId = timelineId,
                ruleVersion = fullResult.ruleVersion,
                analysisVersion = state.version,
                findings = storedFindings,
                computedFromEventId = null,
                isIncremental = false
            ),
            fullResult
        )

        if (drift.hasDrift) {
            logger.warn { "Drift detected for $timelineId: ${drift.mismatches}" }
        } else {
            logger.info { "No drift for $timelineId: ${storedFindings.size} findings match" }
        }

        return drift
    }

    suspend fun repairIfDrifted(timelineId: String): DriftResult {
        val drift = checkTimeline(timelineId)
        if (drift.hasDrift) {
            val state = repository.loadTimeline(timelineId) ?: return drift
            val fullResult = analyzer.analyzeFull(state)
            repository.saveFindings(fullResult)
            logger.info { "Repaired drift for $timelineId with full recompute" }
        }
        return drift
    }
}
