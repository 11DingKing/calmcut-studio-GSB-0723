package com.calmcut.infrastructure.repository

import com.calmcut.domain.*
import com.calmcut.domain.events.*
import com.calmcut.infrastructure.db.*
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import mu.KotlinLogging
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.transactions.experimental.newSuspendedTransaction
import java.time.Instant
import java.util.UUID

private val logger = KotlinLogging.logger {}
private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }

class StoryboardRepository {

    suspend fun create(storyboard: Storyboard): Storyboard = newSuspendedTransaction {
        val id = storyboard.id.toUUID()
        val now = Instant.now()
        Storyboards.insert {
            it[Storyboards.id] = id
            it[externalId] = storyboard.externalId
            it[title] = storyboard.title
            it[currentVersion] = storyboard.currentVersion
            it[knowledgePointCount] = storyboard.knowledgePointCount
            it[originalKnowledgePoints] = storyboard.originalKnowledgePoints?.let { pts -> json.encodeToString(pts) }
            it[createdAt] = now
            it[updatedAt] = now
        }
        storyboard
    }

    suspend fun findById(id: StoryboardId): Storyboard? = newSuspendedTransaction {
        Storyboards.selectAll().where { Storyboards.id eq id.toUUID() }.firstOrNull()?.toStoryboard()
    }

    suspend fun findByExternalId(externalId: String): Storyboard? = newSuspendedTransaction {
        Storyboards.selectAll().where { Storyboards.externalId eq externalId }.firstOrNull()?.toStoryboard()
    }

    suspend fun getCurrentVersion(id: StoryboardId): Long? = newSuspendedTransaction {
        Storyboards.select(Storyboards.currentVersion)
            .where { Storyboards.id eq id.toUUID() }
            .firstOrNull()
            ?.get(Storyboards.currentVersion)
    }

    suspend fun incrementVersion(id: StoryboardId, expectedVersion: Long): Long = newSuspendedTransaction {
        val updated = Storyboards.update({ (Storyboards.id eq id.toUUID()) and (Storyboards.currentVersion eq expectedVersion) }) {
            it[currentVersion] = expectedVersion + 1
            it[updatedAt] = Instant.now()
        }
        if (updated == 0) {
            throw OptimisticLockException("Storyboard ${id.value} version mismatch: expected $expectedVersion")
        }
        expectedVersion + 1
    }

    suspend fun updateKnowledgePointCount(id: StoryboardId, count: Int): Unit = newSuspendedTransaction {
        Storyboards.update({ Storyboards.id eq id.toUUID() }) {
            it[knowledgePointCount] = count
            it[updatedAt] = Instant.now()
        }
    }

    suspend fun getSegments(storyboardId: StoryboardId): List<StoryboardSegment> = newSuspendedTransaction {
        StoryboardSegments.selectAll()
            .where { StoryboardSegments.storyboardId eq storyboardId.toUUID() }
            .orderBy(StoryboardSegments.segmentOrder, SortOrder.ASC)
            .map { row -> row.toSegment(storyboardId) }
    }

    suspend fun getSegmentsInRange(
        storyboardId: StoryboardId,
        startMs: Long,
        endMs: Long
    ): List<StoryboardSegment> = newSuspendedTransaction {
        StoryboardSegments.selectAll()
            .where {
                (StoryboardSegments.storyboardId eq storyboardId.toUUID()) and
                (StoryboardSegments.startTimeMs less endMs) and
                (StoryboardSegments.endTimeMs greater startMs)
            }
            .orderBy(StoryboardSegments.segmentOrder, SortOrder.ASC)
            .map { row -> row.toSegment(storyboardId) }
    }

    suspend fun insertSegments(storyboardId: StoryboardId, segments: List<StoryboardSegment>): Unit =
        newSuspendedTransaction {
            val now = Instant.now()
            segments.forEach { seg ->
                StoryboardSegments.insert {
                    it[id] = seg.id.toUUID()
                    it[StoryboardSegments.storyboardId] = storyboardId.toUUID()
                    it[segmentOrder] = seg.order
                    it[startTimeMs] = seg.startTimeMs
                    it[endTimeMs] = seg.endTimeMs
                    it[stimulusIntensity] = seg.stimulusIntensity
                    it[hasReversal] = seg.hasReversal
                    it[isKnowledgePoint] = seg.isKnowledgePoint
                    it[knowledgePointId] = seg.knowledgePointId
                    it[hasScrollInducement] = seg.hasScrollInducement
                    it[contentType] = seg.contentType.name
                    it[StoryboardSegments.version] = seg.version
                    it[createdAt] = now
                    it[updatedAt] = now
                }
            }
        }

    suspend fun updateSegment(storyboardId: StoryboardId, segmentId: SegmentId, changes: SegmentChangeSet): Unit =
        newSuspendedTransaction {
            val currentVersion = StoryboardSegments.select(StoryboardSegments.version)
                .where {
                    (StoryboardSegments.storyboardId eq storyboardId.toUUID()) and
                    (StoryboardSegments.id eq segmentId.toUUID())
                }
                .firstOrNull()
                ?.get(StoryboardSegments.version) ?: 1L

            StoryboardSegments.update({
                (StoryboardSegments.storyboardId eq storyboardId.toUUID()) and
                (StoryboardSegments.id eq segmentId.toUUID())
            }) {
                changes.order?.let { v -> it[segmentOrder] = v }
                changes.startTimeMs?.let { v -> it[startTimeMs] = v }
                changes.endTimeMs?.let { v -> it[endTimeMs] = v }
                changes.stimulusIntensity?.let { v -> it[stimulusIntensity] = v }
                changes.hasReversal?.let { v -> it[hasReversal] = v }
                changes.isKnowledgePoint?.let { v -> it[isKnowledgePoint] = v }
                changes.knowledgePointId?.let { v -> it[knowledgePointId] = v }
                changes.hasScrollInducement?.let { v -> it[hasScrollInducement] = v }
                changes.contentType?.let { v -> it[contentType] = v }
                it[StoryboardSegments.version] = currentVersion + 1L
                it[updatedAt] = Instant.now()
            }
        }

    suspend fun deleteSegment(storyboardId: StoryboardId, segmentId: SegmentId): Unit = newSuspendedTransaction {
        StoryboardSegments.deleteWhere {
            (StoryboardSegments.storyboardId eq storyboardId.toUUID()) and
            (StoryboardSegments.id eq segmentId.toUUID())
        }
    }

    suspend fun deleteAllSegments(storyboardId: StoryboardId): Unit = newSuspendedTransaction {
        StoryboardSegments.deleteWhere { StoryboardSegments.storyboardId eq storyboardId.toUUID() }
    }

    suspend fun reorderSegments(storyboardId: StoryboardId, newOrder: List<String>): Unit = newSuspendedTransaction {
        newOrder.forEachIndexed { index, segIdStr ->
            StoryboardSegments.update({
                (StoryboardSegments.storyboardId eq storyboardId.toUUID()) and
                (StoryboardSegments.id eq UUID.fromString(segIdStr))
            }) {
                it[segmentOrder] = index
                it[updatedAt] = Instant.now()
            }
        }
    }

    suspend fun getOriginalKnowledgePoints(id: StoryboardId): List<String>? = newSuspendedTransaction {
        Storyboards.select(Storyboards.originalKnowledgePoints)
            .where { Storyboards.id eq id.toUUID() }
            .firstOrNull()
            ?.get(Storyboards.originalKnowledgePoints)
            ?.let { json.decodeFromString<List<String>>(it) }
    }

    private fun ResultRow.toStoryboard(): Storyboard {
        val id = StoryboardId.from(this[Storyboards.id])
        return Storyboard(
            id = id,
            externalId = this[Storyboards.externalId],
            title = this[Storyboards.title],
            currentVersion = this[Storyboards.currentVersion],
            knowledgePointCount = this[Storyboards.knowledgePointCount],
            originalKnowledgePoints = this[Storyboards.originalKnowledgePoints]?.let { json.decodeFromString<List<String>>(it) },
            segments = emptyList(),
            createdAt = this[Storyboards.createdAt].toEpochMilli(),
            updatedAt = this[Storyboards.updatedAt].toEpochMilli()
        )
    }

    private fun ResultRow.toSegment(storyboardId: StoryboardId): StoryboardSegment = StoryboardSegment(
        id = SegmentId.from(this[StoryboardSegments.id]),
        storyboardId = storyboardId,
        order = this[StoryboardSegments.segmentOrder],
        startTimeMs = this[StoryboardSegments.startTimeMs],
        endTimeMs = this[StoryboardSegments.endTimeMs],
        stimulusIntensity = this[StoryboardSegments.stimulusIntensity],
        hasReversal = this[StoryboardSegments.hasReversal],
        isKnowledgePoint = this[StoryboardSegments.isKnowledgePoint],
        knowledgePointId = this[StoryboardSegments.knowledgePointId],
        hasScrollInducement = this[StoryboardSegments.hasScrollInducement],
        contentType = ContentType.valueOf(this[StoryboardSegments.contentType]),
        version = this[StoryboardSegments.version]
    )
}

class OptimisticLockException(message: String) : RuntimeException(message)
