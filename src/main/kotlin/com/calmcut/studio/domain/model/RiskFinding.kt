package com.calmcut.studio.domain.model

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement

@Serializable
data class RiskFinding(
    val findingId: String,
    val timelineId: String,
    val ruleId: String,
    val ruleVersion: String,
    val severity: Severity,
    val segmentIds: List<String>,
    val timeRangeMs: Pair<Long, Long>? = null,
    val evidence: JsonElement,
    val suggestion: String,
    val analysisVersion: Long
) {
    enum class Severity { INFO, WARNING, CRITICAL }
}

@Serializable
data class RiskAnalysisResult(
    val timelineId: String,
    val ruleVersion: String,
    val analysisVersion: Long,
    val findings: List<RiskFinding>,
    val computedFromEventId: String?,
    val isIncremental: Boolean,
    val driftCheckPassed: Boolean? = null
)

object RiskRules {
    const val REVERSAL_WINDOW = "R001"
    const val CONSECUTIVE_INTENSITY = "R002"
    const val AVG_DURATION = "R003"
    const val DECLINE_INDUCEMENT = "R004"
    const val KNOWLEDGE_BUFFER = "R005"

    val ALL_RULES = listOf(
        REVERSAL_WINDOW,
        CONSECUTIVE_INTENSITY,
        AVG_DURATION,
        DECLINE_INDUCEMENT,
        KNOWLEDGE_BUFFER
    )
}
