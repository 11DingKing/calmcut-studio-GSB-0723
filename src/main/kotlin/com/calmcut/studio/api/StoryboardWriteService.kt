package com.calmcut.studio.api

import com.calmcut.studio.db.DatabaseFactory
import com.calmcut.studio.db.EventStore
import com.calmcut.studio.domain.EventType
import com.calmcut.studio.domain.StoryboardEvent
import java.util.UUID

/**
 * Handles single-event storyboard mutations. Each call appends exactly one
 * versioned event under optimistic locking; the analysis worker later projects
 * and analyses it asynchronously. The write path only guarantees the event is
 * durably logged and enqueued in the outbox.
 */
class StoryboardWriteService(
    private val db: DatabaseFactory,
    private val eventStore: EventStore,
) {
    suspend fun batchImport(req: BatchImportRequest): StoryboardEvent = db.dbQuery {
        eventStore.append(
            StoryboardEvent(
                eventId = UUID.randomUUID().toString(),
                storyboardId = req.storyboardId,
                version = 0,
                type = EventType.BATCH_IMPORT,
                segments = req.segments.map { it.toDomain() },
                knowledgePoints = req.knowledgePoints,
            ),
            expectedVersion = req.expectedVersion,
        )
    }

    suspend fun createSegment(req: CreateSegmentRequest): StoryboardEvent = db.dbQuery {
        eventStore.append(
            StoryboardEvent(
                eventId = UUID.randomUUID().toString(),
                storyboardId = req.storyboardId,
                version = 0,
                type = EventType.SEGMENT_CREATED,
                segments = listOf(req.segment.toDomain()),
            ),
            expectedVersion = req.expectedVersion,
        )
    }

    suspend fun updateSegment(req: UpdateSegmentRequest): StoryboardEvent = db.dbQuery {
        eventStore.append(
            StoryboardEvent(
                eventId = UUID.randomUUID().toString(),
                storyboardId = req.storyboardId,
                version = 0,
                type = EventType.SEGMENT_UPDATED,
                segments = listOf(req.segment.toDomain()),
                segmentId = req.segment.id,
            ),
            expectedVersion = req.expectedVersion,
        )
    }

    suspend fun deleteSegment(req: DeleteSegmentRequest): StoryboardEvent = db.dbQuery {
        eventStore.append(
            StoryboardEvent(
                eventId = UUID.randomUUID().toString(),
                storyboardId = req.storyboardId,
                version = 0,
                type = EventType.SEGMENT_DELETED,
                segmentId = req.segmentId,
            ),
            expectedVersion = req.expectedVersion,
        )
    }
}
