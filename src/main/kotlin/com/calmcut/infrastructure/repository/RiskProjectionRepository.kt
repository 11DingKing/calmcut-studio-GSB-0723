package com.calmcut.infrastructure.repository

import com.calmcut.domain.*
import com.calmcut.infrastructure.db.*
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.transactions.experimental.newSuspendedTransaction
import java.time.Instant
import java.util.UUID

private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }

class RiskProjectionRepository {

    suspend fun saveProjection(result: RiskAnalysisResult): Long = newSuspendedTransaction {
        val storyboardUUID = result.storyboardId.toUUID()
        val now = Instant.now()

        RiskFindings.deleteWhere { RiskFindings.storyboardId eq storyboardUUID }

        val existingProj = RiskProjections.selectAll()
            .where { RiskProjections.storyboardId eq storyboardUUID }
            .firstOrNull()

        val projectionId = if (existingProj != null) {
            val pid = existingProj[RiskProjections.id].value
            RiskProjections.update({ RiskProjections.id eq pid }) {
                it[ruleVersion] = result.ruleVersion
                it[projectionVersion] = result.projectionVersion
                it[lastEventId] = null
                it[computedAt] = now
                it[totalRiskScore] = result.totalRiskScore
                it[resultsJson] = json.encodeToString(result.findings)
            }
            pid
        } else {
            RiskProjections.insertAndGetId {
                it[RiskProjections.storyboardId] = storyboardUUID
                it[ruleVersion] = result.ruleVersion
                it[projectionVersion] = result.projectionVersion
                it[lastEventId] = null
                it[computedAt] = now
                it[totalRiskScore] = result.totalRiskScore
                it[resultsJson] = json.encodeToString(result.findings)
            }.value
        }

        result.findings.forEach { finding ->
            RiskFindings.insert {
                it[RiskFindings.projectionId] = projectionId
                it[RiskFindings.storyboardId] = storyboardUUID
                it[ruleId] = finding.ruleId.name
                it[severity] = finding.severity.name
                it[windowStartMs] = finding.windowStartMs
                it[windowEndMs] = finding.windowEndMs
                it[affectedSegmentIds] = "{${finding.affectedSegmentIds.joinToString(",") { it.value }}}"
                it[evidenceJson] = json.encodeToString(finding.evidence)
                it[suggestion] = finding.suggestion
                it[createdAt] = now
            }
        }

        projectionId
    }

    suspend fun getProjection(storyboardId: StoryboardId): RiskAnalysisResult? = newSuspendedTransaction {
        val proj = RiskProjections.selectAll()
            .where { RiskProjections.storyboardId eq storyboardId.toUUID() }
            .firstOrNull() ?: return@newSuspendedTransaction null

        val findings = RiskFindings.selectAll()
            .where { RiskFindings.storyboardId eq storyboardId.toUUID() }
            .map { row ->
                RiskFinding(
                    ruleId = RuleId.valueOf(row[RiskFindings.ruleId]),
                    severity = RiskSeverity.valueOf(row[RiskFindings.severity]),
                    windowStartMs = row[RiskFindings.windowStartMs],
                    windowEndMs = row[RiskFindings.windowEndMs],
                    affectedSegmentIds = parseUUIDArray(row[RiskFindings.affectedSegmentIds]).map { SegmentId.from(it) },
                    evidence = json.decodeFromString<RiskEvidence>(row[RiskFindings.evidenceJson]),
                    suggestion = row[RiskFindings.suggestion]
                )
            }

        RiskAnalysisResult(
            storyboardId = storyboardId,
            ruleVersion = proj[RiskProjections.ruleVersion],
            projectionVersion = proj[RiskProjections.projectionVersion],
            computedAt = proj[RiskProjections.computedAt].toEpochMilli(),
            findings = findings,
            totalRiskScore = proj[RiskProjections.totalRiskScore]
        )
    }

    suspend fun getProjectionVersion(storyboardId: StoryboardId): Long = newSuspendedTransaction {
        RiskProjections.select(RiskProjections.projectionVersion)
            .where { RiskProjections.storyboardId eq storyboardId.toUUID() }
            .firstOrNull()
            ?.get(RiskProjections.projectionVersion) ?: 0L
    }

    suspend fun deleteProjection(storyboardId: StoryboardId): Unit = newSuspendedTransaction {
        val sid = storyboardId.toUUID()
        RiskFindings.deleteWhere { RiskFindings.storyboardId eq sid }
        RiskProjections.deleteWhere { RiskProjections.storyboardId eq sid }
    }

    suspend fun saveDriftCheckpoint(
        storyboardId: StoryboardId,
        checkType: String,
        incrementalResult: RiskAnalysisResult,
        fullResult: RiskAnalysisResult,
        driftDetected: Boolean,
        driftDetails: String?
    ): Unit = newSuspendedTransaction {
        ProjectionDriftCheckpoints.insert {
            it[ProjectionDriftCheckpoints.storyboardId] = storyboardId.toUUID()
            it[ProjectionDriftCheckpoints.checkType] = checkType
            it[ProjectionDriftCheckpoints.incrementalResult] = json.encodeToString(incrementalResult)
            it[ProjectionDriftCheckpoints.fullResult] = json.encodeToString(fullResult)
            it[ProjectionDriftCheckpoints.driftDetected] = driftDetected
            it[ProjectionDriftCheckpoints.driftDetails] = driftDetails
            it[checkedAt] = Instant.now()
        }
    }

    private fun parseUUIDArray(str: String): List<UUID> {
        return str.trim('{', '}')
            .split(",")
            .filter { it.isNotBlank() }
            .map { UUID.fromString(it.trim()) }
    }
}
