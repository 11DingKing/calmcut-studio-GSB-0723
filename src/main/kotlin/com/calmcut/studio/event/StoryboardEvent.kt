package com.calmcut.studio.event

import com.calmcut.studio.domain.model.StoryboardSegment
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import java.util.UUID

@Serializable
sealed interface StoryboardEvent {
    val eventId: String
    val timelineId: String
    val aggregateId: String
    val version: Long
    val eventType: String
    val timestamp: Long

    companion object {
        const val TYPE_TIMELINE_CREATED = "TIMELINE_CREATED"
        const val TYPE_SEGMENT_CREATED = "SEGMENT_CREATED"
        const val TYPE_SEGMENT_UPDATED = "SEGMENT_UPDATED"
        const val TYPE_SEGMENT_DELETED = "SEGMENT_DELETED"
        const val TYPE_BATCH_IMPORTED = "BATCH_IMPORTED"
        const val TYPE_TIMELINE_RESET = "TIMELINE_RESET"
    }
}

@Serializable
@SerialName("TIMELINE_CREATED")
data class TimelineCreated(
    override val eventId: String = UUID.randomUUID().toString(),
    override val timelineId: String,
    override val aggregateId: String = timelineId,
    override val version: Long = 1,
    override val timestamp: Long = System.currentTimeMillis(),
    val initialSegments: List<StoryboardSegment> = emptyList()
) : StoryboardEvent {
    override val eventType: String = StoryboardEvent.TYPE_TIMELINE_CREATED
}

@Serializable
@SerialName("SEGMENT_CREATED")
data class SegmentCreated(
    override val eventId: String = UUID.randomUUID().toString(),
    override val timelineId: String,
    override val aggregateId: String,
    override val version: Long,
    override val timestamp: Long = System.currentTimeMillis(),
    val segment: StoryboardSegment
) : StoryboardEvent {
    override val eventType: String = StoryboardEvent.TYPE_SEGMENT_CREATED
}

@Serializable
@SerialName("SEGMENT_UPDATED")
data class SegmentUpdated(
    override val eventId: String = UUID.randomUUID().toString(),
    override val timelineId: String,
    override val aggregateId: String,
    override val version: Long,
    override val timestamp: Long = System.currentTimeMillis(),
    val previousVersion: StoryboardSegment,
    val segment: StoryboardSegment,
    val expectedVersion: Long
) : StoryboardEvent {
    override val eventType: String = StoryboardEvent.TYPE_SEGMENT_UPDATED
}

@Serializable
@SerialName("SEGMENT_DELETED")
data class SegmentDeleted(
    override val eventId: String = UUID.randomUUID().toString(),
    override val timelineId: String,
    override val aggregateId: String,
    override val version: Long,
    override val timestamp: Long = System.currentTimeMillis(),
    val segmentId: String,
    val expectedVersion: Long
) : StoryboardEvent {
    override val eventType: String = StoryboardEvent.TYPE_SEGMENT_DELETED
}

@Serializable
@SerialName("BATCH_IMPORTED")
data class BatchImported(
    override val eventId: String = UUID.randomUUID().toString(),
    override val timelineId: String,
    override val aggregateId: String = timelineId,
    override val version: Long,
    override val timestamp: Long = System.currentTimeMillis(),
    val jobId: String? = null,
    val segments: List<StoryboardSegment>,
    val mode: ImportMode = ImportMode.REPLACE
) : StoryboardEvent {
    override val eventType: String = StoryboardEvent.TYPE_BATCH_IMPORTED

    @Serializable
    enum class ImportMode { APPEND, REPLACE }
}

@Serializable
@SerialName("TIMELINE_RESET")
data class TimelineReset(
    override val eventId: String = UUID.randomUUID().toString(),
    override val timelineId: String,
    override val aggregateId: String = timelineId,
    override val version: Long,
    override val timestamp: Long = System.currentTimeMillis()
) : StoryboardEvent {
    override val eventType: String = StoryboardEvent.TYPE_TIMELINE_RESET
}
