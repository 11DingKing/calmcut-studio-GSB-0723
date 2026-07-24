package com.calmcut.studio.projection

import com.calmcut.studio.db.DatabaseFactory
import com.calmcut.studio.domain.model.RiskAnalysisResult
import com.calmcut.studio.domain.model.RiskFinding
import com.calmcut.studio.domain.model.StoryboardSegment
import com.calmcut.studio.domain.model.TimelineState
import com.calmcut.studio.event.SegmentProjectionTable
import com.calmcut.studio.event.TimelineProjectionTable
import com.calmcut.studio.event.RiskFindingTable
import com.calmcut.studio.event.AnalysisCheckpointTable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import java.time.Instant
import java.util.UUID

class ProjectionRepository {

    private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }

    suspend fun loadTimeline(timelineId: String): TimelineState? = DatabaseFactory.dbQuery {
        val tpRow = TimelineProjectionTable.selectAll()
            .where { TimelineProjectionTable.timelineId eq timelineId }
            .singleOrNull() ?: return@dbQuery null

        val segments = SegmentProjectionTable.selectAll()
            .where { SegmentProjectionTable.timelineId eq timelineId }
            .orderBy(SegmentProjectionTable.orderIndex)
            .map { row ->
                StoryboardSegment(
                    id = row[SegmentProjectionTable.segmentId],
                    timelineId = row[SegmentProjectionTable.timelineId],
                    version = row[SegmentProjectionTable.version],
                    orderIndex = row[SegmentProjectionTable.orderIndex],
                    startTimeMs = row[SegmentProjectionTable.startTimeMs],
                    endTimeMs = row[SegmentProjectionTable.endTimeMs],
                    intensity = row[SegmentProjectionTable.intensity],
                    isReversal = row[SegmentProjectionTable.isReversal],
                    isKnowledgePoint = row[SegmentProjectionTable.isKnowledgePoint],
                    isDeclineInducement = row[SegmentProjectionTable.isDeclineInducement],
                    knowledgePointId = row[SegmentProjectionTable.knowledgePointId],
                    isOriginal = row[SegmentProjectionTable.isOriginal],
                    content = row[SegmentProjectionTable.content]
                )
            }

        TimelineState(
            timelineId = timelineId,
            version = tpRow[TimelineProjectionTable.currentVersion],
            segments = segments,
            lastEventId = tpRow[TimelineProjectionTable.lastEventId]?.toString()
        )
    }

    suspend fun upsertTimeline(state: TimelineState) = DatabaseFactory.dbQuery {
        val existing = TimelineProjectionTable.selectAll()
            .where { TimelineProjectionTable.timelineId eq state.timelineId }
            .singleOrNull()

        if (existing == null) {
            TimelineProjectionTable.insert {
                it[timelineId] = state.timelineId
                it[currentVersion] = state.version
                it[totalSegments] = state.segments.size
                it[totalDurationMs] = state.totalDurationMs
                it[lastEventId] = state.lastEventId?.let { id -> UUID.fromString(id) }
                it[updatedAt] = Instant.now()
                it[createdAt] = Instant.now()
            }
        } else {
            TimelineProjectionTable.update({ TimelineProjectionTable.timelineId eq state.timelineId }) {
                it[currentVersion] = state.version
                it[totalSegments] = state.segments.size
                it[totalDurationMs] = state.totalDurationMs
                it[lastEventId] = state.lastEventId?.let { id -> UUID.fromString(id) }
                it[updatedAt] = Instant.now()
            }
        }
    }

    suspend fun replaceSegments(timelineId: String, segments: List<StoryboardSegment>) = DatabaseFactory.dbQuery {
        SegmentProjectionTable.deleteWhere { SegmentProjectionTable.timelineId eq timelineId }
        segments.forEach { seg ->
            SegmentProjectionTable.insert {
                it[segmentId] = seg.id
                it[SegmentProjectionTable.timelineId] = seg.timelineId
                it[version] = seg.version
                it[orderIndex] = seg.orderIndex
                it[startTimeMs] = seg.startTimeMs
                it[endTimeMs] = seg.endTimeMs
                it[intensity] = seg.intensity
                it[isReversal] = seg.isReversal
                it[isKnowledgePoint] = seg.isKnowledgePoint
                it[isDeclineInducement] = seg.isDeclineInducement
                it[knowledgePointId] = seg.knowledgePointId
                it[isOriginal] = seg.isOriginal
                it[content] = seg.content
                it[updatedAt] = Instant.now()
            }
        }
    }

    suspend fun upsertSegment(segment: StoryboardSegment) = DatabaseFactory.dbQuery {
        val existing = SegmentProjectionTable.selectAll()
            .where {
                (SegmentProjectionTable.segmentId eq segment.id) and
                    (SegmentProjectionTable.timelineId eq segment.timelineId)
            }
            .singleOrNull()

        if (existing == null) {
            SegmentProjectionTable.insert {
                it[segmentId] = segment.id
                it[timelineId] = segment.timelineId
                it[version] = segment.version
                it[orderIndex] = segment.orderIndex
                it[startTimeMs] = segment.startTimeMs
                it[endTimeMs] = segment.endTimeMs
                it[intensity] = segment.intensity
                it[isReversal] = segment.isReversal
                it[isKnowledgePoint] = segment.isKnowledgePoint
                it[isDeclineInducement] = segment.isDeclineInducement
                it[knowledgePointId] = segment.knowledgePointId
                it[isOriginal] = segment.isOriginal
                it[content] = segment.content
                it[createdAt] = Instant.now()
                it[updatedAt] = Instant.now()
            }
        } else {
            SegmentProjectionTable.update({
                (SegmentProjectionTable.segmentId eq segment.id) and
                    (SegmentProjectionTable.timelineId eq segment.timelineId)
            }) {
                it[version] = segment.version
                it[orderIndex] = segment.orderIndex
                it[startTimeMs] = segment.startTimeMs
                it[endTimeMs] = segment.endTimeMs
                it[intensity] = segment.intensity
                it[isReversal] = segment.isReversal
                it[isKnowledgePoint] = segment.isKnowledgePoint
                it[isDeclineInducement] = segment.isDeclineInducement
                it[knowledgePointId] = segment.knowledgePointId
                it[isOriginal] = segment.isOriginal
                it[content] = segment.content
                it[updatedAt] = Instant.now()
            }
        }
    }

    suspend fun deleteSegment(timelineId: String, segmentId: String) = DatabaseFactory.dbQuery {
        SegmentProjectionTable.deleteWhere {
            (SegmentProjectionTable.timelineId eq timelineId) and (SegmentProjectionTable.segmentId eq segmentId)
        }
    }

    suspend fun saveFindings(result: RiskAnalysisResult) = DatabaseFactory.dbQuery {
        RiskFindingTable.deleteWhere { RiskFindingTable.timelineId eq result.timelineId }
        result.findings.forEach { f ->
            RiskFindingTable.insert {
                it[findingId] = UUID.fromString(f.findingId)
                it[timelineId] = f.timelineId
                it[ruleId] = f.ruleId
                it[ruleVersion] = f.ruleVersion
                it[severity] = f.severity.name
                it[segmentIds] = json.encodeToString(f.segmentIds)
                it[timeRangeStart] = f.timeRangeMs?.first
                it[timeRangeEnd] = f.timeRangeMs?.second
                it[evidence] = f.evidence.toString()
                it[suggestion] = f.suggestion
                it[analysisVersion] = f.analysisVersion
                it[createdAt] = Instant.now()
            }
        }
    }

    suspend fun loadFindings(timelineId: String, analysisVersion: Long? = null): List<RiskFinding> = DatabaseFactory.dbQuery {
        val query = RiskFindingTable.selectAll().where {
            if (analysisVersion != null) {
                (RiskFindingTable.timelineId eq timelineId) and (RiskFindingTable.analysisVersion eq analysisVersion)
            } else {
                RiskFindingTable.timelineId eq timelineId
            }
        }
        query.map { row ->
            RiskFinding(
                findingId = row[RiskFindingTable.findingId].toString(),
                timelineId = row[RiskFindingTable.timelineId],
                ruleId = row[RiskFindingTable.ruleId],
                ruleVersion = row[RiskFindingTable.ruleVersion],
                severity = RiskFinding.Severity.valueOf(row[RiskFindingTable.severity]),
                segmentIds = json.decodeFromString(row[RiskFindingTable.segmentIds]),
                timeRangeMs = row[RiskFindingTable.timeRangeStart]?.let { it to (row[RiskFindingTable.timeRangeEnd] ?: it) },
                evidence = json.parseToJsonElement(row[RiskFindingTable.evidence]),
                suggestion = row[RiskFindingTable.suggestion],
                analysisVersion = row[RiskFindingTable.analysisVersion]
            )
        }
    }

    suspend fun saveCheckpoint(timelineId: String, lastEventId: String, analysisVersion: Long) = DatabaseFactory.dbQuery {
        val existing = AnalysisCheckpointTable.selectAll()
            .where { AnalysisCheckpointTable.timelineId eq timelineId }
            .singleOrNull()
        if (existing == null) {
            AnalysisCheckpointTable.insert {
                it[AnalysisCheckpointTable.timelineId] = timelineId
                it[AnalysisCheckpointTable.lastEventId] = UUID.fromString(lastEventId)
                it[AnalysisCheckpointTable.analysisVersion] = analysisVersion
                it[computedAt] = Instant.now()
            }
        } else {
            AnalysisCheckpointTable.update({ AnalysisCheckpointTable.timelineId eq timelineId }) {
                it[AnalysisCheckpointTable.lastEventId] = UUID.fromString(lastEventId)
                it[AnalysisCheckpointTable.analysisVersion] = analysisVersion
                it[computedAt] = Instant.now()
            }
        }
    }

    suspend fun getCheckpoint(timelineId: String): Triple<String, Long, Any?>? = DatabaseFactory.dbQuery {
        AnalysisCheckpointTable.selectAll()
            .where { AnalysisCheckpointTable.timelineId eq timelineId }
            .singleOrNull()
            ?.let { row ->
                Triple(
                    row[AnalysisCheckpointTable.lastEventId].toString(),
                    row[AnalysisCheckpointTable.analysisVersion],
                    row[AnalysisCheckpointTable.windowState]
                )
            }
    }

    suspend fun resetTimeline(timelineId: String) = DatabaseFactory.dbQuery {
        SegmentProjectionTable.deleteWhere { SegmentProjectionTable.timelineId eq timelineId }
        RiskFindingTable.deleteWhere { RiskFindingTable.timelineId eq timelineId }
        TimelineProjectionTable.update({ TimelineProjectionTable.timelineId eq timelineId }) {
            it[currentVersion] = 0
            it[totalSegments] = 0
            it[totalDurationMs] = 0
            it[updatedAt] = Instant.now()
        }
    }

    suspend fun listTimelines(): List<String> = DatabaseFactory.dbQuery {
        TimelineProjectionTable.selectAll()
            .map { it[TimelineProjectionTable.timelineId] }
    }
}
