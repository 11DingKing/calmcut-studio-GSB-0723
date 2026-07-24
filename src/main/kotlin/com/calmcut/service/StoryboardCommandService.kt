package com.calmcut.service

import com.calmcut.domain.*
import com.calmcut.domain.events.*
import com.calmcut.infrastructure.repository.*
import mu.KotlinLogging
import java.util.UUID

private val logger = KotlinLogging.logger {}

sealed class CommandResult {
    data class Success(val storyboardId: String, val version: Long, val eventId: String, val eventLogId: Long = 0) : CommandResult()
    data class VersionConflict(val expected: Long, val actual: Long) : CommandResult()
    data class ValidationError(val errors: List<String>) : CommandResult()
    data class NotFound(val message: String) : CommandResult()
    data class Error(val message: String) : CommandResult()
}

class StoryboardCommandService(
    private val storyboardRepository: StoryboardRepository,
    private val atomicWriteRepository: AtomicWriteRepository,
    private val eventLogRepository: EventLogRepository,
    private val knowledgePointValidator: KnowledgePointValidator,
    private val eventTopic: String = "storyboard-events"
) {
    suspend fun createStoryboard(
        externalId: String,
        title: String,
        originalKnowledgePoints: List<String>? = null
    ): CommandResult {
        val existing = storyboardRepository.findByExternalId(externalId)
        if (existing != null) {
            return CommandResult.ValidationError(listOf("Storyboard with externalId '$externalId' already exists"))
        }

        val id = StoryboardId.generate()
        val version = 1L
        val event = StoryboardCreated(
            storyboardId = id.value,
            aggregateVersion = version,
            externalId = externalId,
            title = title,
            originalKnowledgePoints = originalKnowledgePoints
        )

        return try {
            val eventLogId = atomicWriteRepository.createStoryboardAtomically(
                storyboardId = id.toUUID(),
                externalId = externalId,
                title = title,
                currentVersion = version,
                originalKnowledgePoints = originalKnowledgePoints,
                event = event,
                topic = eventTopic
            )
            logger.info { "Created storyboard ${id.value} (externalId=$externalId)" }
            CommandResult.Success(id.value, version, event.eventId, eventLogId)
        } catch (e: Exception) {
            logger.error(e) { "Failed to create storyboard" }
            CommandResult.Error(e.message ?: "Unknown error")
        }
    }

    suspend fun addSegment(
        storyboardId: String,
        expectedVersion: Long,
        segment: SegmentData
    ): CommandResult {
        val sid = StoryboardId(storyboardId)

        val errors = validateSegmentData(segment)
        if (errors.isNotEmpty()) return CommandResult.ValidationError(errors)

        val timelineErrors = validateTimeline(sid, listOf(segment.startTimeMs to segment.endTimeMs))
        if (timelineErrors.isNotEmpty()) return CommandResult.ValidationError(timelineErrors)

        val newVersion = expectedVersion + 1
        val event = SegmentAdded(
            storyboardId = storyboardId,
            aggregateVersion = newVersion,
            segment = segment,
            previousSegmentOrder = null
        )

        return executeAtomically(storyboardId, expectedVersion, newVersion, event)
    }

    suspend fun updateSegment(
        storyboardId: String,
        expectedVersion: Long,
        segmentId: String,
        changes: SegmentChangeSet
    ): CommandResult {
        val sid = StoryboardId(storyboardId)

        val errors = validateChangeSet(changes)
        if (errors.isNotEmpty()) return CommandResult.ValidationError(errors)

        val newVersion = expectedVersion + 1
        val event = SegmentUpdated(
            storyboardId = storyboardId,
            aggregateVersion = newVersion,
            segmentId = segmentId,
            changes = changes
        )

        if (changes.startTimeMs != null || changes.endTimeMs != null) {
            val timelineErrors = validateTimeline(sid, null, UUID.fromString(segmentId))
            if (timelineErrors.isNotEmpty()) return CommandResult.ValidationError(timelineErrors)
        }

        return executeAtomically(storyboardId, expectedVersion, newVersion, event)
    }

    suspend fun deleteSegment(
        storyboardId: String,
        expectedVersion: Long,
        segmentId: String
    ): CommandResult {
        val newVersion = expectedVersion + 1
        val event = SegmentDeleted(
            storyboardId = storyboardId,
            aggregateVersion = newVersion,
            segmentId = segmentId,
            segmentOrder = 0
        )
        return executeAtomically(storyboardId, expectedVersion, newVersion, event)
    }

    suspend fun reorderSegments(
        storyboardId: String,
        expectedVersion: Long,
        newOrder: List<String>
    ): CommandResult {
        if (newOrder.isEmpty()) {
            return CommandResult.ValidationError(listOf("newOrder must not be empty"))
        }
        val newVersion = expectedVersion + 1
        val event = SegmentsReordered(
            storyboardId = storyboardId,
            aggregateVersion = newVersion,
            newOrder = newOrder
        )
        return executeAtomically(storyboardId, expectedVersion, newVersion, event)
    }

    suspend fun batchImportSegments(
        storyboardId: String,
        expectedVersion: Long,
        importJobId: String,
        segments: List<SegmentData>
    ): CommandResult {
        val sid = StoryboardId(storyboardId)

        val allErrors = segments.flatMapIndexed { index, seg ->
            validateSegmentData(seg).map { "Segment $index: $it" }
        }
        if (allErrors.isNotEmpty()) return CommandResult.ValidationError(allErrors)

        val newVersion = expectedVersion + 1
        val event = SegmentsBatchImported(
            storyboardId = storyboardId,
            aggregateVersion = newVersion,
            importJobId = importJobId,
            segments = segments,
            batchStartVersion = expectedVersion
        )

        return executeAtomically(storyboardId, expectedVersion, newVersion, event)
    }

    suspend fun requestRebuild(storyboardId: String, reason: String): CommandResult {
        val sid = StoryboardId(storyboardId)
        val currentVersion = storyboardRepository.getCurrentVersion(sid)
            ?: return CommandResult.NotFound("Storyboard not found: $storyboardId")

        val newVersion = currentVersion + 1
        val event = ProjectionRebuildRequested(
            storyboardId = storyboardId,
            aggregateVersion = newVersion,
            reason = reason
        )

        return try {
            val eventLogId = atomicWriteRepository.appendEventAndOutboxAtomically(event, eventTopic)
            CommandResult.Success(storyboardId, newVersion, event.eventId, eventLogId)
        } catch (e: Exception) {
            logger.error(e) { "Failed to request rebuild" }
            CommandResult.Error(e.message ?: "Unknown error")
        }
    }

    private suspend fun executeAtomically(
        storyboardId: String,
        expectedVersion: Long,
        newVersion: Long,
        event: DomainEvent
    ): CommandResult {
        return try {
            val eventLogId = atomicWriteRepository.appendEventAndOutboxAtomically(
                event = event,
                topic = eventTopic,
                expectedVersion = expectedVersion,
                storyboardIdForVersion = storyboardId
            )
            logger.debug { "Atomically wrote event ${event.eventType} v$newVersion for $storyboardId" }
            CommandResult.Success(storyboardId, newVersion, event.eventId, eventLogId)
        } catch (e: OptimisticLockException) {
            val actual = storyboardRepository.getCurrentVersion(StoryboardId(storyboardId)) ?: 0L
            CommandResult.VersionConflict(expectedVersion, actual)
        } catch (e: Exception) {
            logger.error(e) { "Failed to execute command for $storyboardId" }
            CommandResult.Error(e.message ?: "Unknown error")
        }
    }

    private suspend fun validateTimeline(
        storyboardId: StoryboardId,
        newSegments: List<Pair<Long, Long>>? = null,
        excludeSegmentId: UUID? = null
    ): List<String> {
        val result = atomicWriteRepository.validateTimelineContinuous(
            storyboardId.toUUID(),
            newSegments,
            excludeSegmentId
        )
        val errors = mutableListOf<String>()
        if (result.hasOverlaps) {
            errors.add("Timeline has overlapping segments: ${result.overlappingPairs.size} overlap(s) detected")
        }
        return errors
    }

    private fun validateSegmentData(segment: SegmentData): List<String> {
        val errors = mutableListOf<String>()
        if (segment.endTimeMs <= segment.startTimeMs) {
            errors.add("endTimeMs must be greater than startTimeMs")
        }
        if (segment.stimulusIntensity !in 1..5) {
            errors.add("stimulusIntensity must be between 1 and 5")
        }
        if (segment.isKnowledgePoint && segment.knowledgePointId == null) {
            errors.add("knowledgePointId is required when isKnowledgePoint is true")
        }
        if (!segment.isKnowledgePoint && segment.knowledgePointId != null) {
            errors.add("knowledgePointId must be null when isKnowledgePoint is false")
        }
        try {
            ContentType.valueOf(segment.contentType)
        } catch (e: IllegalArgumentException) {
            errors.add("Invalid contentType: ${segment.contentType}")
        }
        return errors
    }

    private fun validateChangeSet(changes: SegmentChangeSet): List<String> {
        val errors = mutableListOf<String>()
        changes.startTimeMs?.let { if (it < 0) errors.add("startTimeMs must be non-negative") }
        changes.endTimeMs?.let { if (it < 0) errors.add("endTimeMs must be non-negative") }
        changes.stimulusIntensity?.let { if (it !in 1..5) errors.add("stimulusIntensity must be between 1 and 5") }
        changes.contentType?.let {
            try { ContentType.valueOf(it) } catch (e: IllegalArgumentException) {
                errors.add("Invalid contentType: $it")
            }
        }
        return errors
    }
}
