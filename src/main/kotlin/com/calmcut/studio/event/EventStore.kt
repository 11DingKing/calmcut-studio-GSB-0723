package com.calmcut.studio.event

import com.calmcut.studio.db.DatabaseFactory
import kotlinx.serialization.encodeToString
import mu.KotlinLogging
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.SqlExpressionBuilder.greater
import java.time.Instant
import java.util.UUID

private val logger = KotlinLogging.logger {}

class EventStore {

    suspend fun append(event: StoryboardEvent, topic: String): String = DatabaseFactory.dbQuery {
        val eventId = UUID.fromString(event.eventId)
        val payloadStr = eventJson.encodeToString(event)

        EventLogTable.insert {
            it[EventLogTable.eventId] = eventId
            it[timelineId] = event.timelineId
            it[aggregateId] = event.aggregateId
            it[eventType] = event.eventType
            it[eventVersion] = event.version
            it[payload] = payloadStr
            it[createdAt] = Instant.now()
        }

        OutboxTable.insert {
            it[OutboxTable.eventId] = eventId
            it[aggregateId] = event.aggregateId
            it[OutboxTable.topic] = topic
            it[key] = event.timelineId
            it[payload] = payloadStr
            it[published] = false
            it[createdAt] = Instant.now()
        }

        logger.debug { "Appended event ${event.eventType} id=${event.eventId} version=${event.version}" }
        event.eventId
    }

    suspend fun getEvent(eventId: String): Pair<String, Long>? = DatabaseFactory.dbQuery {
        EventLogTable.selectAll()
            .where { EventLogTable.eventId eq UUID.fromString(eventId) }
            .map { it[EventLogTable.payload] to it[EventLogTable.eventVersion] }
            .singleOrNull()
    }

    suspend fun getEventsForTimeline(timelineId: String, fromVersion: Long = 0): List<Pair<String, Long>> = DatabaseFactory.dbQuery {
        EventLogTable.selectAll()
            .where {
                (EventLogTable.timelineId eq timelineId) and (EventLogTable.eventVersion greater fromVersion)
            }
            .orderBy(EventLogTable.eventVersion)
            .map { it[EventLogTable.payload] to it[EventLogTable.eventVersion] }
    }

    suspend fun getAllEvents(fromId: UUID? = null, limit: Int = 1000): List<Pair<String, Long>> = DatabaseFactory.dbQuery {
        val query = EventLogTable.selectAll()
        if (fromId != null) {
            query.where { EventLogTable.eventId greater fromId }
        }
        query.orderBy(EventLogTable.eventId)
            .limit(limit)
            .map { it[EventLogTable.payload] to it[EventLogTable.eventVersion] }
    }

    suspend fun getLatestVersion(aggregateId: String): Long = DatabaseFactory.dbQuery {
        EventLogTable.selectAll()
            .where { EventLogTable.aggregateId eq aggregateId }
            .maxOfOrNull { it[EventLogTable.eventVersion] } ?: 0L
    }

    suspend fun getUnpublishedOutbox(limit: Int): List<OutboxEntry> = DatabaseFactory.dbQuery {
        OutboxTable.selectAll()
            .where { OutboxTable.published eq false }
            .orderBy(OutboxTable.id)
            .limit(limit)
            .map { row ->
                OutboxEntry(
                    id = row[OutboxTable.id],
                    eventId = row[OutboxTable.eventId].toString(),
                    aggregateId = row[OutboxTable.aggregateId],
                    topic = row[OutboxTable.topic],
                    key = row[OutboxTable.key],
                    payload = row[OutboxTable.payload],
                    headers = row[OutboxTable.headers]
                )
            }
    }

    suspend fun markPublished(id: Long) = DatabaseFactory.dbQuery {
        OutboxTable.update({ OutboxTable.id eq id }) {
            it[published] = true
            it[publishedAt] = Instant.now()
        }
    }

    suspend fun getDistinctTimelineIds(): List<String> = DatabaseFactory.dbQuery {
        EventLogTable.selectAll()
            .map { it[EventLogTable.timelineId] }
            .distinct()
    }

    data class OutboxEntry(
        val id: Long,
        val eventId: String,
        val aggregateId: String,
        val topic: String,
        val key: String,
        val payload: String,
        val headers: String
    )
}
