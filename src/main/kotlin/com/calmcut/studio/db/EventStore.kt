package com.calmcut.studio.db

import com.calmcut.studio.domain.KnowledgePointSet
import com.calmcut.studio.domain.StoryboardEvent
import com.calmcut.studio.projection.KnowledgePointValidator
import com.calmcut.studio.projection.StoryboardProjector
import com.calmcut.studio.projection.StoryboardState
import java.time.Instant
import org.jetbrains.exposed.sql.SortOrder
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.upsert

/** Raised when an event's expected version does not match the storyboard head. */
class OptimisticLockException(message: String) : RuntimeException(message)

/** Raised when knowledge-point integrity validation fails on a write. */
class IntegrityViolationException(val violations: List<String>) :
    RuntimeException("knowledge point integrity violation: ${violations.joinToString("; ")}")

/**
 * Append-only event store. Every write appends to the event log and the outbox
 * atomically inside one transaction, enforces optimistic locking against
 * [StoryboardHead], and validates knowledge-point integrity. It must be called
 * inside a [DatabaseFactory.dbQuery] transaction.
 */
class EventStore(private val eventsTopic: String) {

    /**
     * Appends [event] at [expectedVersion] (the caller's last-seen head version).
     * The stored version becomes expectedVersion + 1. Fails with
     * [OptimisticLockException] on a version mismatch (concurrent writer).
     */
    fun append(event: StoryboardEvent, expectedVersion: Long): StoryboardEvent {
        val current = currentHeadVersion(event.storyboardId)
        if (current != expectedVersion) {
            throw OptimisticLockException(
                "expected version $expectedVersion for ${event.storyboardId} but head is $current"
            )
        }
        val newVersion = current + 1
        val stored = event.copy(version = newVersion)

        // Validate knowledge-point integrity when the event carries a KP set.
        stored.knowledgePoints?.let {
            val result = KnowledgePointValidator.validate(it)
            if (!result.ok) throw IntegrityViolationException(result.violations)
        }

        val payload = storyboardJson.encodeToString(StoryboardEvent.serializer(), stored)
        val now = Instant.now()

        StoryboardEvents.insert {
            it[eventId] = stored.eventId
            it[storyboardId] = stored.storyboardId
            it[version] = newVersion
            it[eventType] = stored.type.name
            it[this.payload] = payload
            it[createdAt] = now
        }
        Outbox.insert {
            it[eventId] = stored.eventId
            it[storyboardId] = stored.storyboardId
            it[version] = newVersion
            it[topic] = eventsTopic
            it[this.payload] = payload
            it[published] = false
            it[createdAt] = now
        }
        StoryboardHead.upsert(StoryboardHead.storyboardId) {
            it[storyboardId] = stored.storyboardId
            it[version] = newVersion
            it[updatedAt] = now
            stored.knowledgePoints?.let { kp ->
                it[revisionKp] = storyboardJson.encodeToString(KnowledgePointSet.serializer(), kp)
            }
        }
        return stored
    }

    fun currentHeadVersion(storyboardId: String): Long =
        StoryboardHead
            .select(StoryboardHead.version)
            .where { StoryboardHead.storyboardId eq storyboardId }
            .firstOrNull()?.get(StoryboardHead.version) ?: 0L

    /** Reads the ordered event log for a storyboard, used to rebuild projections. */
    fun readLog(storyboardId: String): List<StoryboardEvent> =
        StoryboardEvents
            .selectAll()
            .where { StoryboardEvents.storyboardId eq storyboardId }
            .orderBy(StoryboardEvents.version to SortOrder.ASC)
            .map { storyboardJson.decodeFromString(StoryboardEvent.serializer(), it[StoryboardEvents.payload]) }

    /** Rebuilds authoritative state by folding the persisted log from zero. */
    fun rebuildState(storyboardId: String): StoryboardState =
        StoryboardProjector.rebuild(storyboardId, readLog(storyboardId))
}
