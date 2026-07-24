package com.calmcut.domain.events

import com.calmcut.domain.*
import kotlinx.serialization.Serializable
import java.util.UUID

@Serializable
sealed class DomainEvent {
    abstract val eventId: String
    abstract val eventType: String
    abstract val storyboardId: String
    abstract val aggregateVersion: Long
    abstract val occurredAt: Long
}

@Serializable
data class StoryboardCreated(
    override val eventId: String = UUID.randomUUID().toString(),
    override val storyboardId: String,
    override val aggregateVersion: Long,
    val externalId: String,
    val title: String,
    val originalKnowledgePoints: List<String>? = null,
    override val occurredAt: Long = System.currentTimeMillis()
) : DomainEvent() {
    override val eventType: String = "STORYBOARD_CREATED"
}

@Serializable
data class SegmentsBatchImported(
    override val eventId: String = UUID.randomUUID().toString(),
    override val storyboardId: String,
    override val aggregateVersion: Long,
    val importJobId: String,
    val segments: List<SegmentData>,
    val batchStartVersion: Long,
    override val occurredAt: Long = System.currentTimeMillis()
) : DomainEvent() {
    override val eventType: String = "SEGMENTS_BATCH_IMPORTED"
}

@Serializable
data class SegmentAdded(
    override val eventId: String = UUID.randomUUID().toString(),
    override val storyboardId: String,
    override val aggregateVersion: Long,
    val segment: SegmentData,
    val previousSegmentOrder: Int? = null,
    override val occurredAt: Long = System.currentTimeMillis()
) : DomainEvent() {
    override val eventType: String = "SEGMENT_ADDED"
}

@Serializable
data class SegmentUpdated(
    override val eventId: String = UUID.randomUUID().toString(),
    override val storyboardId: String,
    override val aggregateVersion: Long,
    val segmentId: String,
    val changes: SegmentChangeSet,
    override val occurredAt: Long = System.currentTimeMillis()
) : DomainEvent() {
    override val eventType: String = "SEGMENT_UPDATED"
}

@Serializable
data class SegmentDeleted(
    override val eventId: String = UUID.randomUUID().toString(),
    override val storyboardId: String,
    override val aggregateVersion: Long,
    val segmentId: String,
    val segmentOrder: Int,
    override val occurredAt: Long = System.currentTimeMillis()
) : DomainEvent() {
    override val eventType: String = "SEGMENT_DELETED"
}

@Serializable
data class SegmentsReordered(
    override val eventId: String = UUID.randomUUID().toString(),
    override val storyboardId: String,
    override val aggregateVersion: Long,
    val newOrder: List<String>,
    override val occurredAt: Long = System.currentTimeMillis()
) : DomainEvent() {
    override val eventType: String = "SEGMENTS_REORDERED"
}

@Serializable
data class SegmentData(
    val id: String? = null,
    val order: Int,
    val startTimeMs: Long,
    val endTimeMs: Long,
    val stimulusIntensity: Int,
    val hasReversal: Boolean = false,
    val isKnowledgePoint: Boolean = false,
    val knowledgePointId: String? = null,
    val hasScrollInducement: Boolean = false,
    val contentType: String = "CONTENT"
)

@Serializable
data class SegmentChangeSet(
    val order: Int? = null,
    val startTimeMs: Long? = null,
    val endTimeMs: Long? = null,
    val stimulusIntensity: Int? = null,
    val hasReversal: Boolean? = null,
    val isKnowledgePoint: Boolean? = null,
    val knowledgePointId: String? = null,
    val hasScrollInducement: Boolean? = null,
    val contentType: String? = null
)

@Serializable
data class ImportJobCreated(
    override val eventId: String = UUID.randomUUID().toString(),
    override val storyboardId: String,
    override val aggregateVersion: Long,
    val importJobId: String,
    val totalSegments: Int,
    override val occurredAt: Long = System.currentTimeMillis()
) : DomainEvent() {
    override val eventType: String = "IMPORT_JOB_CREATED"
}

@Serializable
data class ImportJobCompleted(
    override val eventId: String = UUID.randomUUID().toString(),
    override val storyboardId: String,
    override val aggregateVersion: Long,
    val importJobId: String,
    val totalProcessed: Int,
    val totalFailed: Int,
    override val occurredAt: Long = System.currentTimeMillis()
) : DomainEvent() {
    override val eventType: String = "IMPORT_JOB_COMPLETED"
}

@Serializable
data class ImportJobCancelled(
    override val eventId: String = UUID.randomUUID().toString(),
    override val storyboardId: String,
    override val aggregateVersion: Long,
    val importJobId: String,
    val checkpoint: Int,
    override val occurredAt: Long = System.currentTimeMillis()
) : DomainEvent() {
    override val eventType: String = "IMPORT_JOB_CANCELLED"
}

@Serializable
data class ProjectionRebuildRequested(
    override val eventId: String = UUID.randomUUID().toString(),
    override val storyboardId: String,
    override val aggregateVersion: Long,
    val reason: String,
    override val occurredAt: Long = System.currentTimeMillis()
) : DomainEvent() {
    override val eventType: String = "PROJECTION_REBUILD_REQUESTED"
}

fun DomainEvent.toEventEnvelope(): EventEnvelope = EventEnvelope(
    eventId = eventId,
    eventType = eventType,
    storyboardId = storyboardId,
    aggregateVersion = aggregateVersion,
    occurredAt = occurredAt,
    payload = kotlinx.serialization.json.Json.encodeToString(
        DomainEvent.serializer(), this
    )
)

@Serializable
data class EventEnvelope(
    val eventId: String,
    val eventType: String,
    val storyboardId: String,
    val aggregateVersion: Long,
    val occurredAt: Long,
    val payload: String
)
