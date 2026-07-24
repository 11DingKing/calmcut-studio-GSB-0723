package com.calmcut.service

import com.calmcut.domain.*
import com.calmcut.domain.events.*
import com.calmcut.domain.rules.*
import com.calmcut.infrastructure.repository.RiskProjectionRepository
import com.calmcut.infrastructure.repository.StoryboardRepository
import mu.KotlinLogging

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

        val findings = rules.flatMap { rule -> rule.evaluate(sortedSegments) }
            .sortedWith(compareBy({ it.windowStartMs ?: 0 }, { it.ruleId.name }))

        val result = RiskAnalysisResult(
            storyboardId = storyboardId,
            ruleVersion = config.ruleVersion,
            projectionVersion = version,
            computedAt = System.currentTimeMillis(),
            findings = findings,
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

        val dirtyRange = determineDirtyRange(event)

        if (dirtyRange == null) {
            return analyzeFull(storyboardId, version)
        }

        val allSegments = storyboardRepository.getSegments(storyboardId).sortedBy { it.startTimeMs }
        val currentProjection = projectionRepository.getProjection(storyboardId)

        val newFindingsInDirtyRange = rules.flatMap { rule ->
            rule.evaluate(allSegments, dirtyRange)
        }

        val oldFindingsOutsideDirtyRange = if (currentProjection != null) {
            currentProjection.findings.filter { finding ->
                !findingOverlapsRange(finding, dirtyRange)
            }
        } else {
            emptyList()
        }

        val mergedFindings = (oldFindingsOutsideDirtyRange + newFindingsInDirtyRange)
            .distinctBy { findingKey(it) }
            .sortedWith(compareBy({ it.windowStartMs ?: 0 }, { it.ruleId.name }))

        val result = RiskAnalysisResult(
            storyboardId = storyboardId,
            ruleVersion = config.ruleVersion,
            projectionVersion = version,
            computedAt = System.currentTimeMillis(),
            findings = mergedFindings,
            totalRiskScore = calculateRiskScore(mergedFindings),
            affectedWindowStartMs = dirtyRange.start,
            affectedWindowEndMs = dirtyRange.endInclusive
        )

        projectionRepository.saveProjection(result)
        return result
    }

    suspend fun verifyEquivalence(storyboardId: StoryboardId, version: Long): DriftCheckResult {
        val segments = storyboardRepository.getSegments(storyboardId).sortedBy { it.startTimeMs }

        val fullFindings = rules.flatMap { it.evaluate(segments) }
            .sortedWith(compareBy({ it.windowStartMs ?: 0 }, { it.ruleId.name }))

        val fullResult = RiskAnalysisResult(
            storyboardId = storyboardId,
            ruleVersion = config.ruleVersion,
            projectionVersion = version,
            computedAt = System.currentTimeMillis(),
            findings = fullFindings,
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

        val fullKeySet = fullResult.findings.map { findingKey(it) }.toSet()
        val incrementalKeySet = currentProjection.findings.map { findingKey(it) }.toSet()

        val missingFromIncremental = fullKeySet - incrementalKeySet
        val extraInIncremental = incrementalKeySet - fullKeySet

        val hasDrift = missingFromIncremental.isNotEmpty() || extraInIncremental.isNotEmpty()
        val driftDetails = buildString {
            if (missingFromIncremental.isNotEmpty()) {
                appendLine("Missing from incremental projection (${missingFromIncremental.size} findings):")
                missingFromIncremental.take(20).forEach { appendLine("  - $it") }
                if (missingFromIncremental.size > 20) appendLine("  ... and ${missingFromIncremental.size - 20} more")
            }
            if (extraInIncremental.isNotEmpty()) {
                appendLine("Extra in incremental projection (${extraInIncremental.size} findings):")
                extraInIncremental.take(20).forEach { appendLine("  - $it") }
                if (extraInIncremental.size > 20) appendLine("  ... and ${extraInIncremental.size - 20} more")
            }
        }

        if (hasDrift) {
            logger.warn { "Drift detected for storyboard ${storyboardId.value}: ${missingFromIncremental.size} missing, ${extraInIncremental.size} extra" }
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
            message = if (hasDrift) driftDetails else "Incremental and full analysis match exactly (${fullFindings.size} findings)",
            fullResult = fullResult,
            incrementalResult = currentProjection
        )
    }

    private fun findingOverlapsRange(finding: RiskFinding, range: LongRange): Boolean {
        val fStart = finding.windowStartMs ?: return true
        val fEnd = finding.windowEndMs ?: return true
        val extendedRange = (range.start - config.windowSizeMs)..(range.endInclusive + config.windowSizeMs)
        return fEnd >= extendedRange.start && fStart <= extendedRange.endInclusive
    }

    private fun findingKey(f: RiskFinding): String =
        "${f.ruleId}:${f.windowStartMs}:${f.windowEndMs}:${f.evidence.description.hashCode()}:${f.affectedSegmentIds.map { it.value }.sorted().joinToString(",")}"

    private fun determineDirtyRange(event: DomainEvent): LongRange? {
        val w = config.windowSizeMs
        return when (event) {
            is StoryboardCreated -> null
            is ProjectionRebuildRequested -> null
            is ImportJobCreated -> null
            is ImportJobCancelled -> null
            is ImportJobCompleted -> null
            is SegmentsBatchImported -> 0L..Long.MAX_VALUE
            is SegmentAdded -> {
                val s = event.segment
                (s.startTimeMs - w)..(s.endTimeMs + w)
            }
            is SegmentUpdated -> {
                val c = event.changes
                val start = c.startTimeMs
                val end = c.endTimeMs
                if (start != null && end != null) {
                    (start - w)..(end + w)
                } else if (start != null) {
                    (start - w)..(start + w)
                } else {
                    null
                }
            }
            is SegmentDeleted -> null
            is SegmentsReordered -> null
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
