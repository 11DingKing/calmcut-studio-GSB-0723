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

class EventLogRepository {

    suspend fun append(event: DomainEvent): Long = newSuspendedTransaction {
        val eventId = UUID.fromString(event.eventId)
        val existing = EventLog.selectAll().where { EventLog.eventId eq eventId }.firstOrNull()
        if (existing != null) {
            return@newSuspendedTransaction existing[EventLog.id].value
        }

        val payloadStr = when (event) {
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

        EventLog.insertAndGetId {
            it[EventLog.eventId] = eventId
            it[eventType] = event.eventType
            it[storyboardId] = UUID.fromString(event.storyboardId)
            it[aggregateVersion] = event.aggregateVersion
            it[payload] = payloadStr
            it[occurredAt] = Instant.ofEpochMilli(event.occurredAt)
        }.value
    }

    suspend fun appendOutbox(event: DomainEvent, topic: String, headers: Map<String, String> = emptyMap()): Long =
        newSuspendedTransaction {
            val eventId = UUID.fromString(event.eventId)
            val payloadStr = when (event) {
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

            OutboxEvents.insertAndGetId {
                it[OutboxEvents.eventId] = eventId
                it[eventType] = event.eventType
                it[OutboxEvents.topic] = topic
                it[key] = event.storyboardId
                it[payload] = payloadStr
                it[OutboxEvents.headers] = json.encodeToString(headers)
                it[createdAt] = Instant.now()
            }.value
        }

    suspend fun getUnpublishedOutboxEvents(limit: Int = 100): List<OutboxEventRecord> = newSuspendedTransaction {
        OutboxEvents.selectAll()
            .where { OutboxEvents.publishedAt.isNull() }
            .orderBy(OutboxEvents.id, SortOrder.ASC)
            .limit(limit)
            .map { row ->
                OutboxEventRecord(
                    id = row[OutboxEvents.id].value,
                    eventId = row[OutboxEvents.eventId].toString(),
                    eventType = row[OutboxEvents.eventType],
                    topic = row[OutboxEvents.topic],
                    key = row[OutboxEvents.key],
                    payload = row[OutboxEvents.payload],
                    headers = json.decodeFromString<Map<String, String>>(row[OutboxEvents.headers]),
                    attempts = row[OutboxEvents.attempts]
                )
            }
    }

    suspend fun markOutboxPublished(id: Long): Unit = newSuspendedTransaction {
        OutboxEvents.update({ OutboxEvents.id eq id }) {
            it[publishedAt] = Instant.now()
        }
    }

    suspend fun markOutboxFailed(id: Long, error: String): Unit = newSuspendedTransaction {
        val current = OutboxEvents.select(OutboxEvents.attempts)
            .where { OutboxEvents.id eq id }
            .firstOrNull()
            ?.get(OutboxEvents.attempts) ?: 0
        OutboxEvents.update({ OutboxEvents.id eq id }) {
            it[attempts] = current + 1
            it[lastError] = error
        }
    }

    suspend fun markProcessed(eventLogId: Long): Unit = newSuspendedTransaction {
        EventLog.update({ EventLog.id eq eventLogId }) {
            it[processedAt] = Instant.now()
        }
    }

    suspend fun getEventsForStoryboardAfterVersion(
        storyboardId: String,
        afterVersion: Long,
        limit: Int = 1000
    ): List<DomainEvent> = newSuspendedTransaction {
        EventLog.selectAll()
            .where {
                (EventLog.storyboardId eq UUID.fromString(storyboardId)) and
                (EventLog.aggregateVersion greater afterVersion)
            }
            .orderBy(EventLog.aggregateVersion, SortOrder.ASC)
            .limit(limit)
            .map { decodeEvent(it[EventLog.eventType], it[EventLog.payload]) }
    }

    suspend fun getLatestVersion(storyboardId: String): Long = newSuspendedTransaction {
        EventLog.select(EventLog.aggregateVersion)
            .where { EventLog.storyboardId eq UUID.fromString(storyboardId) }
            .orderBy(EventLog.aggregateVersion, SortOrder.DESC)
            .limit(1)
            .firstOrNull()
            ?.get(EventLog.aggregateVersion) ?: 0L
    }

    suspend fun getAllEventsForStoryboard(storyboardId: String): List<DomainEvent> = newSuspendedTransaction {
        EventLog.selectAll()
            .where { EventLog.storyboardId eq UUID.fromString(storyboardId) }
            .orderBy(EventLog.aggregateVersion, SortOrder.ASC)
            .map { decodeEvent(it[EventLog.eventType], it[EventLog.payload]) }
    }

    private fun decodeEvent(eventType: String, payload: String): DomainEvent = when (eventType) {
        "STORYBOARD_CREATED" -> json.decodeFromString<StoryboardCreated>(payload)
        "SEGMENTS_BATCH_IMPORTED" -> json.decodeFromString<SegmentsBatchImported>(payload)
        "SEGMENT_ADDED" -> json.decodeFromString<SegmentAdded>(payload)
        "SEGMENT_UPDATED" -> json.decodeFromString<SegmentUpdated>(payload)
        "SEGMENT_DELETED" -> json.decodeFromString<SegmentDeleted>(payload)
        "SEGMENTS_REORDERED" -> json.decodeFromString<SegmentsReordered>(payload)
        "IMPORT_JOB_CREATED" -> json.decodeFromString<ImportJobCreated>(payload)
        "IMPORT_JOB_COMPLETED" -> json.decodeFromString<ImportJobCompleted>(payload)
        "IMPORT_JOB_CANCELLED" -> json.decodeFromString<ImportJobCancelled>(payload)
        "PROJECTION_REBUILD_REQUESTED" -> json.decodeFromString<ProjectionRebuildRequested>(payload)
        else -> throw IllegalArgumentException("Unknown event type: $eventType")
    }
}

data class OutboxEventRecord(
    val id: Long,
    val eventId: String,
    val eventType: String,
    val topic: String,
    val key: String,
    val payload: String,
    val headers: Map<String, String>,
    val attempts: Int
)
