package com.calmcut.domain.rules

import com.calmcut.domain.*
import kotlinx.serialization.Serializable

@Serializable
data class RiskRuleConfig(
    val ruleVersion: String = "1.0.0",
    val windowSizeSeconds: Int = 60,
    val maxReversalsPerWindow: Int = 6,
    val consecutiveHighStimulus: Int = 3,
    val highStimulusThreshold: Int = 5,
    val minAverageShotSeconds: Double = 3.0,
    val bufferStimulusThreshold: Int = 2
) {
    val windowSizeMs: Long get() = windowSizeSeconds.toLong() * 1000L
}

interface RiskRule {
    val ruleId: RuleId
    val config: RiskRuleConfig

    fun evaluate(
        segments: List<StoryboardSegment>,
        affectedRangeMs: LongRange? = null
    ): List<RiskFinding>

    fun affectedWindowsByChange(
        changeStartMs: Long,
        changeEndMs: Long,
        totalDurationMs: Long
    ): List<LongRange> {
        val windowSize = config.windowSizeMs
        val earliestStart = maxOf(0, changeStartMs - windowSize)
        val latestStart = minOf(totalDurationMs - windowSize, changeEndMs)

        if (earliestStart > latestStart && totalDurationMs < windowSize) {
            return listOf(0..totalDurationMs)
        }

        val windows = mutableListOf<LongRange>()
        var cursor = earliestStart
        while (cursor <= latestStart) {
            val end = minOf(cursor + windowSize, totalDurationMs)
            windows.add(cursor..end)
            cursor += 1000L
        }
        return windows
    }
}

object RiskRules {
    fun allRules(config: RiskRuleConfig): List<RiskRule> = listOf(
        ExcessiveReversalsRule(config),
        ConsecutiveHighStimulusRule(config),
        ShortShotDurationRule(config),
        ScrollInducementRule(config),
        MissingBufferRule(config)
    )
}

class ExcessiveReversalsRule(override val config: RiskRuleConfig) : RiskRule {
    override val ruleId = RuleId.EXCESSIVE_REVERSALS_IN_WINDOW

    override fun evaluate(
        segments: List<StoryboardSegment>,
        affectedRangeMs: LongRange?
    ): List<RiskFinding> {
        val findings = mutableListOf<RiskFinding>()
        val windowMs = config.windowSizeMs
        val maxReversals = config.maxReversalsPerWindow
        val sorted = segments.sortedBy { it.startTimeMs }

        if (sorted.isEmpty()) return findings

        val totalDuration = sorted.last().endTimeMs
        val stepMs = 1000L
        val effectiveWindowMs = minOf(windowMs, totalDuration)
        val maxStart = if (totalDuration <= windowMs) 0L else totalDuration - windowMs + stepMs

        var windowStart = 0L
        while (windowStart <= maxStart) {
            val windowEnd = windowStart + effectiveWindowMs
            if (affectedRangeMs != null && windowEnd < affectedRangeMs.start) {
                windowStart += stepMs
                continue
            }
            if (affectedRangeMs != null && windowStart > affectedRangeMs.endInclusive) {
                break
            }

            val segmentsInWindow = sorted.filter { seg ->
                seg.startTimeMs < windowEnd && seg.endTimeMs > windowStart
            }
            val reversalCount = segmentsInWindow.count { it.hasReversal }

            if (reversalCount > maxReversals) {
                findings.add(
                    RiskFinding(
                        ruleId = ruleId,
                        severity = if (reversalCount > maxReversals + 2) RiskSeverity.HIGH else RiskSeverity.MEDIUM,
                        windowStartMs = windowStart,
                        windowEndMs = windowEnd,
                        affectedSegmentIds = segmentsInWindow.filter { it.hasReversal }.map { it.id },
                        evidence = RiskEvidence(
                            description = "在 ${windowMs / 1000} 秒窗口内检测到 $reversalCount 次强反转，超过阈值 $maxReversals 次",
                            metrics = mapOf(
                                "windowSizeSeconds" to (windowMs / 1000).toDouble(),
                                "reversalCount" to reversalCount.toDouble(),
                                "threshold" to maxReversals.toDouble()
                            ),
                            segmentDetails = segmentsInWindow.filter { it.hasReversal }.map { seg ->
                                SegmentEvidence(
                                    segmentId = seg.id.value,
                                    order = seg.order,
                                    startTimeMs = seg.startTimeMs,
                                    endTimeMs = seg.endTimeMs,
                                    relevantFields = mapOf("hasReversal" to "true", "intensity" to seg.stimulusIntensity.toString())
                                )
                            }
                        ),
                        suggestion = "建议在该时间段内减少强反转次数至 ${maxReversals} 次以内，可通过增加过渡片段或降低反转强度来优化节奏。"
                    )
                )
            }
            windowStart += stepMs
        }
        return dedupeFindings(findings)
    }
}

class ConsecutiveHighStimulusRule(override val config: RiskRuleConfig) : RiskRule {
    override val ruleId = RuleId.CONSECUTIVE_HIGH_STIMULUS

    override fun evaluate(
        segments: List<StoryboardSegment>,
        affectedRangeMs: LongRange?
    ): List<RiskFinding> {
        val findings = mutableListOf<RiskFinding>()
        val sorted = segments.sortedBy { it.order }
        val threshold = config.consecutiveHighStimulus
        val intensityThreshold = config.highStimulusThreshold

        if (sorted.size < threshold) return findings

        var i = 0
        while (i <= sorted.size - threshold) {
            val window = sorted.subList(i, i + threshold)
            val allHigh = window.all { it.stimulusIntensity >= intensityThreshold }

            if (allHigh) {
                val rangeStart = window.first().startTimeMs
                val rangeEnd = window.last().endTimeMs

                if (affectedRangeMs != null && rangeEnd < affectedRangeMs.start) {
                    i++
                    continue
                }
                if (affectedRangeMs != null && rangeStart > affectedRangeMs.endInclusive) {
                    break
                }

                var extend = i + threshold
                while (extend < sorted.size && sorted[extend].stimulusIntensity >= intensityThreshold) {
                    extend++
                }
                val fullRun = sorted.subList(i, extend)

                findings.add(
                    RiskFinding(
                        ruleId = ruleId,
                        severity = if (fullRun.size > threshold + 1) RiskSeverity.HIGH else RiskSeverity.MEDIUM,
                        windowStartMs = fullRun.first().startTimeMs,
                        windowEndMs = fullRun.last().endTimeMs,
                        affectedSegmentIds = fullRun.map { it.id },
                        evidence = RiskEvidence(
                            description = "检测到连续 ${fullRun.size} 段刺激强度达到 $intensityThreshold 或更高，超过连续 $threshold 段的阈值",
                            metrics = mapOf(
                                "consecutiveCount" to fullRun.size.toDouble(),
                                "threshold" to threshold.toDouble(),
                                "intensityThreshold" to intensityThreshold.toDouble()
                            ),
                            segmentDetails = fullRun.map { seg ->
                                SegmentEvidence(
                                    segmentId = seg.id.value,
                                    order = seg.order,
                                    startTimeMs = seg.startTimeMs,
                                    endTimeMs = seg.endTimeMs,
                                    relevantFields = mapOf("intensity" to seg.stimulusIntensity.toString())
                                )
                            }
                        ),
                        suggestion = "建议在连续高刺激段之间插入低刺激缓冲段（强度≤${config.bufferStimulusThreshold}），帮助观众缓解视觉疲劳。"
                    )
                )
                i = extend
            } else {
                i++
            }
        }
        return findings
    }
}

class ShortShotDurationRule(override val config: RiskRuleConfig) : RiskRule {
    override val ruleId = RuleId.SHOT_DURATION_TOO_SHORT

    override fun evaluate(
        segments: List<StoryboardSegment>,
        affectedRangeMs: LongRange?
    ): List<RiskFinding> {
        val findings = mutableListOf<RiskFinding>()
        val sorted = segments.sortedBy { it.startTimeMs }

        if (sorted.isEmpty()) return findings

        val windowMs = config.windowSizeMs
        val minAvgSeconds = config.minAverageShotSeconds
        val stepMs = 1000L

        val totalDuration = sorted.last().endTimeMs
        val effectiveWindowMs = minOf(windowMs, totalDuration)

        var windowStart = 0L
        val maxStart = if (totalDuration <= windowMs) 0L else totalDuration - windowMs + stepMs

        while (windowStart <= maxStart) {
            val windowEnd = windowStart + effectiveWindowMs

            if (affectedRangeMs != null && windowEnd < affectedRangeMs.start) {
                windowStart += stepMs
                continue
            }
            if (affectedRangeMs != null && windowStart > affectedRangeMs.endInclusive) {
                break
            }

            val segmentsInWindow = sorted.filter { seg ->
                seg.startTimeMs < windowEnd && seg.endTimeMs > windowStart
            }

            if (segmentsInWindow.size < 2) {
                windowStart += stepMs
                continue
            }

            val actualWindowStart = maxOf(windowStart, segmentsInWindow.first().startTimeMs)
            val actualWindowEnd = minOf(windowEnd, segmentsInWindow.last().endTimeMs)
            val windowDurationSeconds = (actualWindowEnd - actualWindowStart) / 1000.0
            val avgShotSeconds = windowDurationSeconds / segmentsInWindow.size

            if (avgShotSeconds < minAvgSeconds) {
                findings.add(
                    RiskFinding(
                        ruleId = ruleId,
                        severity = if (avgShotSeconds < minAvgSeconds * 0.5) RiskSeverity.HIGH else RiskSeverity.MEDIUM,
                        windowStartMs = windowStart,
                        windowEndMs = windowEnd,
                        affectedSegmentIds = segmentsInWindow.map { it.id },
                        evidence = RiskEvidence(
                            description = "在 ${windowMs / 1000} 秒窗口内平均镜头时长为 ${"%.2f".format(avgShotSeconds)} 秒，短于最低要求 $minAvgSeconds 秒",
                            metrics = mapOf(
                                "windowSizeSeconds" to (windowMs / 1000).toDouble(),
                                "averageShotSeconds" to avgShotSeconds,
                                "minThresholdSeconds" to minAvgSeconds,
                                "segmentCount" to segmentsInWindow.size.toDouble()
                            ),
                            segmentDetails = segmentsInWindow.map { seg ->
                                SegmentEvidence(
                                    segmentId = seg.id.value,
                                    order = seg.order,
                                    startTimeMs = seg.startTimeMs,
                                    endTimeMs = seg.endTimeMs,
                                    relevantFields = mapOf(
                                        "durationSeconds" to "%.2f".format(seg.durationSeconds),
                                        "intensity" to seg.stimulusIntensity.toString()
                                    )
                                )
                            }
                        ),
                        suggestion = "建议将该区域内镜头平均时长提高至至少 ${minAvgSeconds} 秒，可通过合并相似镜头或延长关键镜头来实现。"
                    )
                )
            }
            windowStart += stepMs
        }
        return dedupeFindings(findings)
    }
}

class ScrollInducementRule(override val config: RiskRuleConfig) : RiskRule {
    override val ruleId = RuleId.SCROLL_INDUCEMENT_PRESENT

    override fun evaluate(
        segments: List<StoryboardSegment>,
        affectedRangeMs: LongRange?
    ): List<RiskFinding> {
        return segments
            .filter { it.hasScrollInducement }
            .filter { seg ->
                affectedRangeMs == null ||
                    (seg.endTimeMs >= affectedRangeMs.start && seg.startTimeMs <= affectedRangeMs.endInclusive)
            }
            .map { seg ->
                RiskFinding(
                    ruleId = ruleId,
                    severity = RiskSeverity.LOW,
                    windowStartMs = seg.startTimeMs,
                    windowEndMs = seg.endTimeMs,
                    affectedSegmentIds = listOf(seg.id),
                    evidence = RiskEvidence(
                        description = "检测到含有继续下滑诱导元素的片段",
                        metrics = mapOf(
                            "segmentOrder" to seg.order.toDouble(),
                            "durationSeconds" to seg.durationSeconds
                        ),
                        segmentDetails = listOf(
                            SegmentEvidence(
                                segmentId = seg.id.value,
                                order = seg.order,
                                startTimeMs = seg.startTimeMs,
                                endTimeMs = seg.endTimeMs,
                                relevantFields = mapOf("hasScrollInducement" to "true")
                            )
                        )
                    ),
                    suggestion = "建议移除或弱化继续下滑诱导元素，避免刻意引导用户持续观看，提升内容自然体验。"
                )
            }
    }
}

class MissingBufferRule(override val config: RiskRuleConfig) : RiskRule {
    override val ruleId = RuleId.MISSING_BUFFER_BETWEEN_KNOWLEDGE_POINTS

    override fun evaluate(
        segments: List<StoryboardSegment>,
        affectedRangeMs: LongRange?
    ): List<RiskFinding> {
        val findings = mutableListOf<RiskFinding>()
        val sorted = segments.sortedBy { it.order }

        val knowledgePointIndices = sorted.mapIndexedNotNull { index, seg ->
            if (seg.isKnowledgePoint) index to seg else null
        }

        if (knowledgePointIndices.size < 2) return findings

        for (i in 0 until knowledgePointIndices.size - 1) {
            val (currentIdx, currentKp) = knowledgePointIndices[i]
            val (nextIdx, nextKp) = knowledgePointIndices[i + 1]

            val rangeStart = currentKp.endTimeMs
            val rangeEnd = nextKp.startTimeMs

            if (affectedRangeMs != null && rangeEnd < affectedRangeMs.start) continue
            if (affectedRangeMs != null && rangeStart > affectedRangeMs.endInclusive) break

            if (nextIdx == currentIdx + 1) {
                findings.add(
                    createMissingBufferFinding(currentKp, nextKp, emptyList())
                )
            } else {
                val betweenSegments = sorted.subList(currentIdx + 1, nextIdx)
                val hasAdequateBuffer = betweenSegments.any { seg ->
                    seg.stimulusIntensity <= config.bufferStimulusThreshold &&
                    seg.durationMs >= 1000L
                }
                if (!hasAdequateBuffer) {
                    findings.add(
                        createMissingBufferFinding(currentKp, nextKp, betweenSegments)
                    )
                }
            }
        }
        return findings
    }

    private fun createMissingBufferFinding(
        currentKp: StoryboardSegment,
        nextKp: StoryboardSegment,
        betweenSegments: List<StoryboardSegment>
    ): RiskFinding {
        return RiskFinding(
            ruleId = ruleId,
            severity = RiskSeverity.MEDIUM,
            windowStartMs = currentKp.endTimeMs,
            windowEndMs = nextKp.startTimeMs,
            affectedSegmentIds = listOf(currentKp.id, nextKp.id) + betweenSegments.map { it.id },
            evidence = RiskEvidence(
                description = "知识点「${currentKp.knowledgePointId}」与「${nextKp.knowledgePointId}」之间缺少低刺激缓冲段",
                metrics = mapOf(
                    "gapDurationSeconds" to ((nextKp.startTimeMs - currentKp.endTimeMs) / 1000.0),
                    "bufferStimulusThreshold" to config.bufferStimulusThreshold.toDouble(),
                    "betweenSegmentCount" to betweenSegments.size.toDouble()
                ),
                segmentDetails = buildList {
                    add(
                        SegmentEvidence(
                            segmentId = currentKp.id.value,
                            order = currentKp.order,
                            startTimeMs = currentKp.startTimeMs,
                            endTimeMs = currentKp.endTimeMs,
                            relevantFields = mapOf("knowledgePointId" to (currentKp.knowledgePointId ?: ""), "role" to "knowledge_point_start")
                        )
                    )
                    betweenSegments.forEach { seg ->
                        add(
                            SegmentEvidence(
                                segmentId = seg.id.value,
                                order = seg.order,
                                startTimeMs = seg.startTimeMs,
                                endTimeMs = seg.endTimeMs,
                                relevantFields = mapOf("intensity" to seg.stimulusIntensity.toString(), "role" to "between")
                            )
                        )
                    }
                    add(
                        SegmentEvidence(
                            segmentId = nextKp.id.value,
                            order = nextKp.order,
                            startTimeMs = nextKp.startTimeMs,
                            endTimeMs = nextKp.endTimeMs,
                            relevantFields = mapOf("knowledgePointId" to (nextKp.knowledgePointId ?: ""), "role" to "knowledge_point_end")
                        )
                    )
                }
            ),
            suggestion = "建议在两个知识点之间插入至少 1 秒的低刺激（强度≤${config.bufferStimulusThreshold}）过渡/缓冲段，给观众留出消化知识的时间。"
        )
    }
}

private fun dedupeFindings(findings: List<RiskFinding>): List<RiskFinding> {
    return findings
        .sortedBy { it.windowStartMs }
        .fold(mutableListOf<RiskFinding>()) { acc, finding ->
            val last = acc.lastOrNull()
            if (last != null &&
                last.ruleId == finding.ruleId &&
                last.windowStartMs != null &&
                finding.windowStartMs != null &&
                finding.windowStartMs - last.windowStartMs < 5000L
            ) {
                acc
            } else {
                acc.add(finding)
                acc
            }
        }
}
