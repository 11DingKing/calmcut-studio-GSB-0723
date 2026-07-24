package com.calmcut.infrastructure.repository

import com.calmcut.domain.events.*
import com.calmcut.infrastructure.db.*
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import mu.KotlinLogging
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.experimental.newSuspendedTransaction
import java.time.Instant
import java.util.UUID

private val logger = KotlinLogging.logger {}
private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }

class AtomicWriteRepository {

    suspend fun <T> inTransaction(block: suspend (Transaction) -> T): T = newSuspendedTransaction {
        block(this)
    }

    suspend fun appendEventAndOutboxAtomically(
        event: DomainEvent,
        topic: String,
        expectedVersion: Long? = null,
        storyboardIdForVersion: String? = null
    ): Long = newSuspendedTransaction {
        val eventId = UUID.fromString(event.eventId)
        val storyboardUUID = event.storyboardId.let { UUID.fromString(it) }

        val duplicate = EventLog.selectAll()
            .where { EventLog.eventId eq eventId }
            .firstOrNull()
        if (duplicate != null) {
            return@newSuspendedTransaction duplicate[EventLog.id].value
        }

        if (expectedVersion != null && storyboardIdForVersion != null) {
            val currentVersion = Storyboards.select(Storyboards.currentVersion)
                .where { Storyboards.id eq UUID.fromString(storyboardIdForVersion) }
                .firstOrNull()
                ?.get(Storyboards.currentVersion) ?: 0L

            if (currentVersion != expectedVersion) {
                throw OptimisticLockException(
                    "Version conflict for $storyboardIdForVersion: expected $expectedVersion, actual $currentVersion"
                )
            }

            val updated = Storyboards.update({
                (Storyboards.id eq UUID.fromString(storyboardIdForVersion)) and
                (Storyboards.currentVersion eq expectedVersion)
            }) {
                it[Storyboards.currentVersion] = event.aggregateVersion
                it[Storyboards.updatedAt] = Instant.now()
            }
            if (updated == 0) {
                throw OptimisticLockException(
                    "Failed to increment version for $storyboardIdForVersion (concurrent modification)"
                )
            }
        }

        val payloadStr = serializeEvent(event)

        val eventLogId = EventLog.insertAndGetId {
            it[EventLog.eventId] = eventId
            it[eventType] = event.eventType
            it[EventLog.storyboardId] = storyboardUUID
            it[aggregateVersion] = event.aggregateVersion
            it[payload] = payloadStr
            it[occurredAt] = Instant.ofEpochMilli(event.occurredAt)
        }.value

        OutboxEvents.insert {
            it[OutboxEvents.eventId] = eventId
            it[eventType] = event.eventType
            it[OutboxEvents.topic] = topic
            it[key] = event.storyboardId
            it[payload] = payloadStr
            it[headers] = "{}"
            it[createdAt] = Instant.now()
        }

        logger.debug { "Atomically wrote event ${event.eventId} (v${event.aggregateVersion}) to event_log and outbox for ${event.storyboardId}" }
        eventLogId
    }

    suspend fun createStoryboardAtomically(
        storyboardId: UUID,
        externalId: String,
        title: String,
        currentVersion: Long,
        originalKnowledgePoints: List<String>?,
        event: StoryboardCreated,
        topic: String
    ): Long = newSuspendedTransaction {
        val now = Instant.now()
        val eventId = UUID.fromString(event.eventId)
        val payloadStr = serializeEvent(event)

        Storyboards.insert {
            it[Storyboards.id] = storyboardId
            it[Storyboards.externalId] = externalId
            it[Storyboards.title] = title
            it[Storyboards.currentVersion] = currentVersion
            it[knowledgePointCount] = 0
            it[Storyboards.originalKnowledgePoints] = originalKnowledgePoints?.let { pts -> json.encodeToString(pts) }
            it[createdAt] = now
            it[updatedAt] = now
        }

        val eventLogId = EventLog.insertAndGetId {
            it[EventLog.eventId] = eventId
            it[eventType] = event.eventType
            it[EventLog.storyboardId] = storyboardId
            it[aggregateVersion] = currentVersion
            it[payload] = payloadStr
            it[occurredAt] = now
        }.value

        OutboxEvents.insert {
            it[OutboxEvents.eventId] = eventId
            it[eventType] = event.eventType
            it[OutboxEvents.topic] = topic
            it[key] = storyboardId.toString()
            it[payload] = payloadStr
            it[headers] = "{}"
            it[createdAt] = now
        }

        eventLogId
    }

    suspend fun markEventProcessed(eventLogId: Long): Unit = newSuspendedTransaction {
        EventLog.update({ EventLog.id eq eventLogId }) {
            it[processedAt] = Instant.now()
        }
    }

    suspend fun isEventProcessed(eventId: String): Boolean = newSuspendedTransaction {
        EventLog.selectAll()
            .where { EventLog.eventId eq UUID.fromString(eventId) }
            .firstOrNull()
            ?.get(EventLog.processedAt) != null
    }

    suspend fun getProcessedVersion(consumerGroup: String, storyboardId: String): Long = newSuspendedTransaction {
        ConsumerStoryboardVersions.select(ConsumerStoryboardVersions.lastProcessedVersion)
            .where {
                (ConsumerStoryboardVersions.consumerGroup eq consumerGroup) and
                (ConsumerStoryboardVersions.storyboardId eq UUID.fromString(storyboardId))
            }
            .firstOrNull()
            ?.get(ConsumerStoryboardVersions.lastProcessedVersion) ?: 0L
    }

    suspend fun saveProcessedVersion(consumerGroup: String, storyboardId: String, version: Long): Unit =
        newSuspendedTransaction {
            val existing = ConsumerStoryboardVersions.selectAll()
                .where {
                    (ConsumerStoryboardVersions.consumerGroup eq consumerGroup) and
                    (ConsumerStoryboardVersions.storyboardId eq UUID.fromString(storyboardId))
                }
                .firstOrNull()

            if (existing != null) {
                ConsumerStoryboardVersions.update({
                    (ConsumerStoryboardVersions.consumerGroup eq consumerGroup) and
                    (ConsumerStoryboardVersions.storyboardId eq UUID.fromString(storyboardId))
                }) {
                    it[lastProcessedVersion] = version
                    it[updatedAt] = Instant.now()
                }
            } else {
                ConsumerStoryboardVersions.insert {
                    it[ConsumerStoryboardVersions.consumerGroup] = consumerGroup
                    it[ConsumerStoryboardVersions.storyboardId] = UUID.fromString(storyboardId)
                    it[lastProcessedVersion] = version
                    it[updatedAt] = Instant.now()
                }
            }
        }

    suspend fun saveProcessedOffset(
        consumerGroup: String,
        topic: String,
        partition: Int,
        offset: Long,
        eventId: String?,
        eventType: String?,
        storyboardId: String?
    ): Unit = newSuspendedTransaction {
        val existing = ConsumerProcessedOffsets.selectAll()
            .where {
                (ConsumerProcessedOffsets.consumerGroup eq consumerGroup) and
                (ConsumerProcessedOffsets.topic eq topic) and
                (ConsumerProcessedOffsets.partition eq partition) and
                (ConsumerProcessedOffsets.offsetVal eq offset)
            }
            .firstOrNull()

        if (existing == null) {
            ConsumerProcessedOffsets.insert {
                it[ConsumerProcessedOffsets.consumerGroup] = consumerGroup
                it[ConsumerProcessedOffsets.topic] = topic
                it[ConsumerProcessedOffsets.partition] = partition
                it[offsetVal] = offset
                it[ConsumerProcessedOffsets.eventId] = eventId?.let { UUID.fromString(it) }
                it[ConsumerProcessedOffsets.eventType] = eventType
                it[ConsumerProcessedOffsets.storyboardId] = storyboardId?.let { UUID.fromString(it) }
                it[processedAt] = Instant.now()
            }
        }
    }

    suspend fun isOffsetProcessed(consumerGroup: String, topic: String, partition: Int, offset: Long): Boolean =
        newSuspendedTransaction {
            ConsumerProcessedOffsets.selectAll()
                .where {
                    (ConsumerProcessedOffsets.consumerGroup eq consumerGroup) and
                    (ConsumerProcessedOffsets.topic eq topic) and
                    (ConsumerProcessedOffsets.partition eq partition) and
                    (ConsumerProcessedOffsets.offsetVal eq offset)
                }
                .count() > 0
        }

    suspend fun validateTimelineContinuous(
        storyboardId: java.util.UUID,
        newSegments: List<Pair<Long, Long>>? = null,
        excludeSegmentId: UUID? = null
    ): TimelineValidationDb {
        return newSuspendedTransaction {
            val existingSegments = StoryboardSegments.select(
                StoryboardSegments.id, StoryboardSegments.startTimeMs, StoryboardSegments.endTimeMs
            )
                .where { StoryboardSegments.storyboardId eq storyboardId }
                .orderBy(StoryboardSegments.startTimeMs, SortOrder.ASC)
                .map {
                    Triple(
                        it[StoryboardSegments.id],
                        it[StoryboardSegments.startTimeMs],
                        it[StoryboardSegments.endTimeMs]
                    )
                }
                .filter { excludeSegmentId == null || it.first != excludeSegmentId }

            val allTimeRanges = existingSegments.map { it.second to it.third } + (newSegments ?: emptyList())
            val sorted = allTimeRanges.sortedBy { it.first }

            val overlaps = mutableListOf<Pair<LongRange, LongRange>>()
            val gaps = mutableListOf<LongRange>()
            val outOfOrder = mutableListOf<String>()

            for (i in 0 until sorted.size - 1) {
                val current = sorted[i]
                val next = sorted[i + 1]
                if (next.first < current.second) {
                    overlaps.add((current.first..current.second) to (next.first..next.second))
                } else if (next.first > current.second) {
                    gaps.add(current.second..next.first)
                }
            }

            TimelineValidationDb(
                hasOverlaps = overlaps.isNotEmpty(),
                overlappingPairs = overlaps,
                gaps = gaps,
                isContinuous = overlaps.isEmpty() && gaps.isEmpty()
            )
        }
    }

    private fun serializeEvent(event: DomainEvent): String = when (event) {
        is StoryboardCreated -> json.encodeToString(event)
        is SegmentsBatchImported -> json.encodeToString(event)
        is SegmentAdded -> json.encodeToString(event)
        is SegmentUpdated -> json.encodeToString(event)
        is SegmentDeleted -> json.encodeToString(event)
        is SegmentsReordered -> json.encodeToString(event)
        is ImportJobCreated -> json.encodeToString(event)
        is ImportJobCompleted -> json.encodeToString(event)
        is ImportJobCancelled -> json.encodeToString(event)
        is ProjectionRebuildRequested -> json.encodeToString(event)
    }
}

data class TimelineValidationDb(
    val hasOverlaps: Boolean,
    val overlappingPairs: List<Pair<LongRange, LongRange>>,
    val gaps: List<LongRange>,
    val isContinuous: Boolean
)
