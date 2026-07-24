package com.calmcut.studio.analysis.rules

import com.calmcut.studio.analysis.*
import com.calmcut.studio.config.AppConfig
import com.calmcut.studio.domain.model.RiskFinding
import com.calmcut.studio.domain.model.RiskRules
import com.calmcut.studio.domain.model.StoryboardSegment
import kotlinx.serialization.json.*

class ConsecutiveIntensityRule : RiskRule {
    override val ruleId = RiskRules.CONSECUTIVE_INTENSITY
    override val ruleVersion = "1.0.0"
    override val description = "连续 3 段刺激强度为 5"

    override fun evaluateFull(segments: List<StoryboardSegment>, config: AppConfig.AnalysisConfig): List<RiskFinding> {
        val sorted = segments.sortedByOrder()
        val findings = mutableListOf<RiskFinding>()
        val runLength = config.consecutiveIntensityCount

        for (i in 0..sorted.size - runLength) {
            val window = sorted.subList(i, i + runLength)
            if (window.all { it.intensity == config.maxIntensity }) {
                findings.add(
                    RiskFinding(
                        findingId = newFindingId(),
                        timelineId = sorted.first().timelineId,
                        ruleId = ruleId,
                        ruleVersion = ruleVersion,
                        severity = RiskFinding.Severity.WARNING,
                        segmentIds = window.map { it.id },
                        timeRangeMs = window.first().startTimeMs to window.last().endTimeMs,
                        evidence = buildJsonObject {
                            put("startIndex", i)
                            put("runLength", runLength)
                            put("intensity", config.maxIntensity)
                            putJsonArray("segments") {
                                window.forEach { seg ->
                                    addJsonObject {
                                        put("id", seg.id)
                                        put("orderIndex", seg.orderIndex)
                                        put("intensity", seg.intensity)
                                    }
                                }
                            }
                        },
                        suggestion = "连续${runLength}段刺激强度均为最大值${config.maxIntensity}，可能造成观感疲劳。建议在高刺激段落间插入低刺激过渡，降低连续冲击。",
                        analysisVersion = 0L
                    )
                )
            }
        }
        return findings
    }

    override fun affectedRange(
        oldSegments: List<StoryboardSegment>,
        newSegments: List<StoryboardSegment>,
        changedSegmentIds: Set<String>,
        config: AppConfig.AnalysisConfig
    ): AffectedRange {
        val sorted = newSegments.sortedByOrder()
        val indices = changedSegmentIds.mapNotNull { id ->
            sorted.indexOfFirst { it.id == id }.takeIf { it >= 0 }
        }
        if (indices.isEmpty() && changedSegmentIds.isNotEmpty()) {
            val oldSorted = oldSegments.sortedByOrder()
            val oldIdx = changedSegmentIds.mapNotNull { id ->
                oldSorted.indexOfFirst { it.id == id }.takeIf { it >= 0 }
            }
            if (oldIdx.isEmpty()) return AffectedRange.none()
            val minIdx = (oldIdx.min() - (config.consecutiveIntensityCount - 1)).coerceAtLeast(0)
            val maxIdx = (oldIdx.max() + config.consecutiveIntensityCount - 1).coerceAtMost(oldSorted.size - 1)
            return AffectedRange.indices(minIdx..maxIdx)
        }
        val minIdx = (indices.min() - (config.consecutiveIntensityCount - 1)).coerceAtLeast(0)
        val maxIdx = (indices.max() + config.consecutiveIntensityCount - 1).coerceAtMost(sorted.size - 1)
        return AffectedRange.indices(minIdx..maxIdx)
    }

    override fun evaluateIncremental(
        oldSegments: List<StoryboardSegment>,
        newSegments: List<StoryboardSegment>,
        changedSegmentIds: Set<String>,
        previousFindings: List<RiskFinding>,
        config: AppConfig.AnalysisConfig
    ): List<RiskFinding> {
        val range = affectedRange(oldSegments, newSegments, changedSegmentIds, config)
        val idxRange = range.indexRange ?: return if (range.requiresFullRecompute) evaluateFull(newSegments, config) else previousFindings

        val allNewFindings = evaluateFull(newSegments, config)

        val findingsOutsideRange = previousFindings.filter { f ->
            val sorted = newSegments.sortedByOrder()
            val segIndices = f.segmentIds.mapNotNull { id -> sorted.indexOfFirst { it.id == id }.takeIf { it >= 0 } }
            segIndices.isNotEmpty() && segIndices.none { it in idxRange }
        }

        val findingsInsideRange = allNewFindings.filter { f ->
            val sorted = newSegments.sortedByOrder()
            val segIndices = f.segmentIds.mapNotNull { id -> sorted.indexOfFirst { it.id == id }.takeIf { it >= 0 } }
            segIndices.any { it in idxRange }
        }

        return dedupeFindings(findingsOutsideRange + findingsInsideRange)
    }
}
