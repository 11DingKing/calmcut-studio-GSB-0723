package com.calmcut.studio.analysis

/**
 * Tunable thresholds for the five risk rules. Defaults mirror application.conf.
 * Kept as an immutable value so a given [ruleSetVersion] always yields identical
 * findings for identical input — a precondition for incremental==full equivalence.
 */
data class AnalysisSettings(
    val reversalWindowMs: Long = 60_000,
    val maxReversalsPerWindow: Int = 6,
    val consecutiveIntensityValue: Int = 5,
    val consecutiveIntensityCount: Int = 3,
    val minAverageShotMs: Long = 3_000,
    val lowStimulusIntensity: Int = 2,
    val ruleSetVersion: String = RULESET_VERSION,
) {
    companion object {
        const val RULESET_VERSION = "2026.07.1"
    }
}
