package com.calmcut.studio.analysis

import com.calmcut.studio.config.AppConfig
import com.calmcut.studio.domain.model.RiskFinding
import com.calmcut.studio.domain.model.StoryboardSegment
import java.util.UUID

interface RiskRule {
    val ruleId: String
    val ruleVersion: String
    val description: String

    fun evaluateFull(segments: List<StoryboardSegment>, config: AppConfig.AnalysisConfig): List<RiskFinding>

    fun affectedRange(
        oldSegments: List<StoryboardSegment>,
        newSegments: List<StoryboardSegment>,
        changedSegmentIds: Set<String>,
        config: AppConfig.AnalysisConfig
    ): AffectedRange

    fun evaluateIncremental(
        oldSegments: List<StoryboardSegment>,
        newSegments: List<StoryboardSegment>,
        changedSegmentIds: Set<String>,
        previousFindings: List<RiskFinding>,
        config: AppConfig.AnalysisConfig
    ): List<RiskFinding>
}

data class AffectedRange(
    val indexRange: IntRange?,
    val timeRangeMs: LongRange?,
    val requiresFullRecompute: Boolean = false
) {
    companion object {
        fun full() = AffectedRange(indexRange = null, timeRangeMs = null, requiresFullRecompute = true)
        fun indices(range: IntRange) = AffectedRange(indexRange = range, timeRangeMs = null)
        fun time(range: LongRange) = AffectedRange(indexRange = null, timeRangeMs = range)
        fun none() = AffectedRange(indexRange = IntRange.EMPTY, timeRangeMs = LongRange.EMPTY)
    }
}

internal fun newFindingId(): String = UUID.randomUUID().toString()

internal fun List<StoryboardSegment>.sortedByOrder(): List<StoryboardSegment> = sortedBy { it.orderIndex }

internal fun List<StoryboardSegment>.sortedByTime(): List<StoryboardSegment> = sortedBy { it.startTimeMs }

internal inline fun <T> List<T>.pairwise(action: (T, T) -> Unit) {
    for (i in 0 until size - 1) {
        action(this[i], this[i + 1])
    }
}
