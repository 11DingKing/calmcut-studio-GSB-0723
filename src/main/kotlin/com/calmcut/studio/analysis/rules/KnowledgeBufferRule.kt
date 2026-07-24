package com.calmcut.studio.analysis.rules

import com.calmcut.studio.analysis.*
import com.calmcut.studio.config.AppConfig
import com.calmcut.studio.domain.model.RiskFinding
import com.calmcut.studio.domain.model.RiskRules
import com.calmcut.studio.domain.model.StoryboardSegment
import kotlinx.serialization.json.*

class KnowledgeBufferRule : RiskRule {
    override val ruleId = RiskRules.KNOWLEDGE_BUFFER
    override val ruleVersion = "1.0.0"
    override val description = "知识点间缺少低刺激缓冲段"

    override fun evaluateFull(segments: List<StoryboardSegment>, config: AppConfig.AnalysisConfig): List<RiskFinding> {
        val sorted = segments.sortedByOrder()
        val knowledgePoints = sorted.filter { it.isKnowledgePoint }
        if (knowledgePoints.size < 2) return emptyList()

        val findings = mutableListOf<RiskFinding>()

        for (i in 0 until knowledgePoints.size - 1) {
            val kp1 = knowledgePoints[i]
            val kp2 = knowledgePoints[i + 1]

            val betweenStart = sorted.indexOf(kp1) + 1
            val betweenEnd = sorted.indexOf(kp2) - 1
            val between = if (betweenStart <= betweenEnd) sorted.subList(betweenStart, betweenEnd + 1) else emptyList()

            val hasBuffer = between.any { it.intensity <= config.bufferIntensityThreshold }
            val isAdjacent = between.isEmpty()

            if (!hasBuffer) {
                findings.add(
                    RiskFinding(
                        findingId = newFindingId(),
                        timelineId = kp1.timelineId,
                        ruleId = ruleId,
                        ruleVersion = ruleVersion,
                        severity = RiskFinding.Severity.WARNING,
                        segmentIds = listOf(kp1.id, kp2.id) + between.map { it.id },
                        timeRangeMs = kp1.startTimeMs to kp2.endTimeMs,
                        evidence = buildJsonObject {
                            put("kp1Id", kp1.id)
                            put("kp2Id", kp2.id)
                            put("kp1OrderIndex", kp1.orderIndex)
                            put("kp2OrderIndex", kp2.orderIndex)
                            put("isAdjacent", isAdjacent)
                            put("bufferIntensityThreshold", config.bufferIntensityThreshold)
                            put("segmentCountBetween", between.size)
                            putJsonArray("betweenIntensities") {
                                between.forEach { add(it.intensity) }
                            }
                        },
                        suggestion = if (isAdjacent) {
                            "两个知识点（${kp1.id} 和 ${kp2.id}）之间没有缓冲段。建议在知识点之间插入低刺激（强度≤${config.bufferIntensityThreshold}）的过渡内容，帮助观众消化信息。"
                        } else {
                            "两个知识点（${kp1.id} 和 ${kp2.id}）之间的${between.size}段内容均缺少低刺激（≤${config.bufferIntensityThreshold}）缓冲。建议至少加入一段低刺激过渡。"
                        },
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
        val changedIndices = changedSegmentIds.mapNotNull { id ->
            sorted.indexOfFirst { it.id == id }.takeIf { it >= 0 }
        }
        if (changedIndices.isEmpty()) {
            val oldSorted = oldSegments.sortedByOrder()
            val oldIdx = changedSegmentIds.mapNotNull { id ->
                oldSorted.indexOfFirst { it.id == id }.takeIf { it >= 0 }
            }
            if (oldIdx.isEmpty()) return AffectedRange.none()
            val minIdx = (oldIdx.min() - 2).coerceAtLeast(0)
            val maxIdx = (oldIdx.max() + 2).coerceAtMost(oldSorted.size - 1)
            return AffectedRange.indices(minIdx..maxIdx)
        }
        val minIdx = (changedIndices.min() - 2).coerceAtLeast(0)
        val maxIdx = (changedIndices.max() + 2).coerceAtMost(sorted.size - 1)
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

        val sorted = newSegments.sortedByOrder()
        val affectedKpIds = mutableSetOf<String>()
        for (i in idxRange) {
            if (i in sorted.indices && sorted[i].isKnowledgePoint) {
                affectedKpIds.add(sorted[i].id)
            }
        }

        val findingsOutsideRange = previousFindings.filter { f ->
            f.segmentIds.none { it in affectedKpIds }
        }

        val findingsInsideRange = allNewFindings.filter { f ->
            f.segmentIds.any { it in affectedKpIds }
        }

        return dedupeFindings(findingsOutsideRange + findingsInsideRange)
    }
}
