package com.calmcut.service

import com.calmcut.domain.*
import com.calmcut.domain.events.*
import com.calmcut.domain.rules.*
import com.calmcut.infrastructure.repository.RiskProjectionRepository
import com.calmcut.infrastructure.repository.StoryboardRepository
import mu.KotlinLogging
import java.util.concurrent.ConcurrentHashMap

private val logger = KotlinLogging.logger {}

class RiskAnalysisEngine(
    private val config: RiskRuleConfig = RiskRuleConfig(),
    private val storyboardRepository: StoryboardRepository,
    private val projectionRepository: RiskProjectionRepository
) {
    private val rules = RiskRules.allRules(config)

    suspend fun analyzeFull(storyboardId: StoryboardId, version: Long): RiskAnalysisResult {
        logger.debug { "Performing full analysis for storyboard ${storyboardId.value} at version $version" }

        val segments = storyboardRepository.getSegments(storyboardId)
        val sortedSegments = segments.sortedBy { it.startTimeMs }

        val findings = rules.flatMap { rule ->
            rule.evaluate(sortedSegments)
        }

        val result = RiskAnalysisResult(
            storyboardId = storyboardId,
            ruleVersion = config.ruleVersion,
            projectionVersion = version,
            computedAt = System.currentTimeMillis(),
            findings = findings.sortedWith(compareBy({ it.windowStartMs ?: 0 }, { it.ruleId.name })),
            totalRiskScore = calculateRiskScore(findings)
        )

        projectionRepository.saveProjection(result)
        return result
    }

    suspend fun analyzeIncremental(
        storyboardId: StoryboardId,
        version: Long,
        event: DomainEvent
    ): RiskAnalysisResult {
        logger.debug { "Performing incremental analysis for storyboard ${storyboardId.value} at version $version (event=${event.eventType})" }

        val affectedRange = determineAffectedRange(event)
        val segments = if (affectedRange != null) {
            val extendedStart = maxOf(0, affectedRange.start - config.windowSizeMs * 2)
            val extendedEnd = affectedRange.endInclusive + config.windowSizeMs * 2
            storyboardRepository.getSegmentsInRange(storyboardId, extendedStart, extendedEnd)
        } else {
            storyboardRepository.getSegments(storyboardId)
        }

        val allSegments = if (affectedRange != null && segments.isNotEmpty()) {
            storyboardRepository.getSegments(storyboardId).sortedBy { it.startTimeMs }
        } else {
            segments.sortedBy { it.startTimeMs }
        }

        val sortedSegments = allSegments.sortedBy { it.startTimeMs }

        val findings = mutableListOf<RiskFinding>()
        for (rule in rules) {
            val ruleFindings = if (affectedRange != null) {
                rule.evaluate(sortedSegments, affectedRange)
            } else {
                rule.evaluate(sortedSegments)
            }
            findings.addAll(ruleFindings)
        }

        val currentProjection = projectionRepository.getProjection(storyboardId)
        val mergedFindings = if (currentProjection != null && affectedRange != null) {
            val unaffected = currentProjection.findings.filter { finding ->
                val findingStart = finding.windowStartMs
                val findingEnd = finding.windowEndMs
                findingStart == null || findingEnd == null ||
                    findingEnd < affectedRange.start - config.windowSizeMs ||
                    findingStart > affectedRange.endInclusive + config.windowSizeMs
            }
            (unaffected + findings)
                .distinctBy { Triple(it.ruleId, it.windowStartMs, it.affectedSegmentIds.map { id -> id.value }) }
                .sortedWith(compareBy({ it.windowStartMs ?: 0 }, { it.ruleId.name }))
        } else {
            findings.sortedWith(compareBy({ it.windowStartMs ?: 0 }, { it.ruleId.name }))
        }

        val result = RiskAnalysisResult(
            storyboardId = storyboardId,
            ruleVersion = config.ruleVersion,
            projectionVersion = version,
            computedAt = System.currentTimeMillis(),
            findings = mergedFindings,
            totalRiskScore = calculateRiskScore(mergedFindings),
            affectedWindowStartMs = affectedRange?.start,
            affectedWindowEndMs = affectedRange?.endInclusive
        )

        projectionRepository.saveProjection(result)
        return result
    }

    suspend fun verifyEquivalence(storyboardId: StoryboardId, version: Long): DriftCheckResult {
        val segments = storyboardRepository.getSegments(storyboardId)
        val sortedSegments = segments.sortedBy { it.startTimeMs }

        val fullFindings = rules.flatMap { it.evaluate(sortedSegments) }
        val fullResult = RiskAnalysisResult(
            storyboardId = storyboardId,
            ruleVersion = config.ruleVersion,
            projectionVersion = version,
            computedAt = System.currentTimeMillis(),
            findings = fullFindings.sortedWith(compareBy({ it.windowStartMs ?: 0 }, { it.ruleId.name })),
            totalRiskScore = calculateRiskScore(fullFindings)
        )

        val currentProjection = projectionRepository.getProjection(storyboardId)
        if (currentProjection == null) {
            return DriftCheckResult(
                hasDrift = false,
                message = "No existing projection to compare",
                fullResult = fullResult,
                incrementalResult = null
            )
        }

        val incrementalKeySet = currentProjection.findings.map {
            "${it.ruleId}:${it.windowStartMs}:${it.windowEndMs}:${it.evidence.description.hashCode()}"
        }.toSet()

        val fullKeySet = fullResult.findings.map {
            "${it.ruleId}:${it.windowStartMs}:${it.windowEndMs}:${it.evidence.description.hashCode()}"
        }.toSet()

        val missingFromIncremental = fullKeySet - incrementalKeySet
        val extraInIncremental = incrementalKeySet - fullKeySet

        val hasDrift = missingFromIncremental.isNotEmpty() || extraInIncremental.isNotEmpty()
        val driftDetails = buildString {
            if (missingFromIncremental.isNotEmpty()) {
                appendLine("Missing from incremental projection:")
                missingFromIncremental.take(10).forEach { appendLine("  - $it") }
                if (missingFromIncremental.size > 10) appendLine("  ... and ${missingFromIncremental.size - 10} more")
            }
            if (extraInIncremental.isNotEmpty()) {
                appendLine("Extra in incremental projection:")
                extraInIncremental.take(10).forEach { appendLine("  - $it") }
                if (extraInIncremental.size > 10) appendLine("  ... and ${extraInIncremental.size - 10} more")
            }
        }

        if (hasDrift) {
            logger.warn { "Drift detected for storyboard ${storyboardId.value}: $driftDetails" }
            projectionRepository.saveProjection(fullResult)
        }

        projectionRepository.saveDriftCheckpoint(
            storyboardId = storyboardId,
            checkType = "FULL_COMPARISON",
            incrementalResult = currentProjection,
            fullResult = fullResult,
            driftDetected = hasDrift,
            driftDetails = if (hasDrift) driftDetails else null
        )

        return DriftCheckResult(
            hasDrift = hasDrift,
            message = if (hasDrift) driftDetails else "Incremental and full analysis match",
            fullResult = fullResult,
            incrementalResult = currentProjection
        )
    }

    private fun determineAffectedRange(event: DomainEvent): LongRange? {
        return when (event) {
            is StoryboardCreated -> null
            is ProjectionRebuildRequested -> null
            is ImportJobCreated -> null
            is ImportJobCompleted -> null
            is ImportJobCancelled -> null
            is SegmentsBatchImported -> 0L..Long.MAX_VALUE
            is SegmentAdded -> {
                val seg = event.segment
                (seg.startTimeMs - config.windowSizeMs)..(seg.endTimeMs + config.windowSizeMs)
            }
            is SegmentUpdated -> {
                val changes = event.changes
                val start = changes.startTimeMs ?: 0
                val end = changes.endTimeMs ?: (start + 10000)
                (start - config.windowSizeMs)..(end + config.windowSizeMs)
            }
            is SegmentDeleted -> 0L..Long.MAX_VALUE
            is SegmentsReordered -> 0L..Long.MAX_VALUE
        }
    }

    private fun calculateRiskScore(findings: List<RiskFinding>): Double {
        return findings.sumOf { finding ->
            when (finding.severity) {
                RiskSeverity.HIGH -> 10.0
                RiskSeverity.MEDIUM -> 5.0
                RiskSeverity.LOW -> 2.0
            }
        }
    }
}

data class DriftCheckResult(
    val hasDrift: Boolean,
    val message: String,
    val fullResult: RiskAnalysisResult,
    val incrementalResult: RiskAnalysisResult?
)
