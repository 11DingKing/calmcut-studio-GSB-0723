package com.calmcut.service

import com.calmcut.domain.*
import com.calmcut.domain.events.*
import com.calmcut.infrastructure.repository.EventLogRepository
import com.calmcut.infrastructure.repository.OptimisticLockException
import com.calmcut.infrastructure.repository.StoryboardRepository
import mu.KotlinLogging
import java.util.UUID

private val logger = KotlinLogging.logger {}

sealed class CommandResult {
    data class Success(val storyboardId: String, val version: Long, val eventId: String) : CommandResult()
    data class VersionConflict(val expected: Long, val actual: Long) : CommandResult()
    data class ValidationError(val errors: List<String>) : CommandResult()
    data class NotFound(val message: String) : CommandResult()
    data class Error(val message: String) : CommandResult()
}

class StoryboardCommandService(
    private val storyboardRepository: StoryboardRepository,
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
            val storyboard = Storyboard(
                id = id,
                externalId = externalId,
                title = title,
                currentVersion = version,
                originalKnowledgePoints = originalKnowledgePoints,
                segments = emptyList()
            )
            storyboardRepository.create(storyboard)
            eventLogRepository.append(event)
            eventLogRepository.appendOutbox(event, eventTopic)
            logger.info { "Created storyboard ${id.value} (externalId=$externalId)" }
            CommandResult.Success(id.value, version, event.eventId)
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

        return try {
            val newVersion = storyboardRepository.incrementVersion(sid, expectedVersion)

            val event = SegmentAdded(
                storyboardId = storyboardId,
                aggregateVersion = newVersion,
                segment = segment,
                previousSegmentOrder = null
            )

            eventLogRepository.append(event)
            eventLogRepository.appendOutbox(event, eventTopic)

            logger.debug { "Added segment to $storyboardId at version $newVersion" }
            CommandResult.Success(storyboardId, newVersion, event.eventId)
        } catch (e: OptimisticLockException) {
            val actual = storyboardRepository.getCurrentVersion(sid) ?: 0L
            CommandResult.VersionConflict(expectedVersion, actual)
        } catch (e: Exception) {
            logger.error(e) { "Failed to add segment to $storyboardId" }
            CommandResult.Error(e.message ?: "Unknown error")
        }
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

        return try {
            val newVersion = storyboardRepository.incrementVersion(sid, expectedVersion)

            val event = SegmentUpdated(
                storyboardId = storyboardId,
                aggregateVersion = newVersion,
                segmentId = segmentId,
                changes = changes
            )

            eventLogRepository.append(event)
            eventLogRepository.appendOutbox(event, eventTopic)

            logger.debug { "Updated segment $segmentId in $storyboardId at version $newVersion" }
            CommandResult.Success(storyboardId, newVersion, event.eventId)
        } catch (e: OptimisticLockException) {
            val actual = storyboardRepository.getCurrentVersion(sid) ?: 0L
            CommandResult.VersionConflict(expectedVersion, actual)
        } catch (e: Exception) {
            logger.error(e) { "Failed to update segment $segmentId in $storyboardId" }
            CommandResult.Error(e.message ?: "Unknown error")
        }
    }

    suspend fun deleteSegment(
        storyboardId: String,
        expectedVersion: Long,
        segmentId: String
    ): CommandResult {
        val sid = StoryboardId(storyboardId)

        return try {
            val newVersion = storyboardRepository.incrementVersion(sid, expectedVersion)

            val event = SegmentDeleted(
                storyboardId = storyboardId,
                aggregateVersion = newVersion,
                segmentId = segmentId,
                segmentOrder = 0
            )

            eventLogRepository.append(event)
            eventLogRepository.appendOutbox(event, eventTopic)

            logger.debug { "Deleted segment $segmentId from $storyboardId at version $newVersion" }
            CommandResult.Success(storyboardId, newVersion, event.eventId)
        } catch (e: OptimisticLockException) {
            val actual = storyboardRepository.getCurrentVersion(sid) ?: 0L
            CommandResult.VersionConflict(expectedVersion, actual)
        } catch (e: Exception) {
            logger.error(e) { "Failed to delete segment $segmentId from $storyboardId" }
            CommandResult.Error(e.message ?: "Unknown error")
        }
    }

    suspend fun reorderSegments(
        storyboardId: String,
        expectedVersion: Long,
        newOrder: List<String>
    ): CommandResult {
        val sid = StoryboardId(storyboardId)

        if (newOrder.isEmpty()) {
            return CommandResult.ValidationError(listOf("newOrder must not be empty"))
        }

        return try {
            val newVersion = storyboardRepository.incrementVersion(sid, expectedVersion)

            val event = SegmentsReordered(
                storyboardId = storyboardId,
                aggregateVersion = newVersion,
                newOrder = newOrder
            )

            eventLogRepository.append(event)
            eventLogRepository.appendOutbox(event, eventTopic)

            logger.debug { "Reordered segments in $storyboardId at version $newVersion" }
            CommandResult.Success(storyboardId, newVersion, event.eventId)
        } catch (e: OptimisticLockException) {
            val actual = storyboardRepository.getCurrentVersion(sid) ?: 0L
            CommandResult.VersionConflict(expectedVersion, actual)
        } catch (e: Exception) {
            logger.error(e) { "Failed to reorder segments in $storyboardId" }
            CommandResult.Error(e.message ?: "Unknown error")
        }
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

        return try {
            val newVersion = storyboardRepository.incrementVersion(sid, expectedVersion)

            val event = SegmentsBatchImported(
                storyboardId = storyboardId,
                aggregateVersion = newVersion,
                importJobId = importJobId,
                segments = segments,
                batchStartVersion = expectedVersion
            )

            eventLogRepository.append(event)
            eventLogRepository.appendOutbox(event, eventTopic)

            logger.info { "Batch imported ${segments.size} segments to $storyboardId at version $newVersion" }
            CommandResult.Success(storyboardId, newVersion, event.eventId)
        } catch (e: OptimisticLockException) {
            val actual = storyboardRepository.getCurrentVersion(sid) ?: 0L
            CommandResult.VersionConflict(expectedVersion, actual)
        } catch (e: Exception) {
            logger.error(e) { "Failed to batch import to $storyboardId" }
            CommandResult.Error(e.message ?: "Unknown error")
        }
    }

    suspend fun requestRebuild(storyboardId: String, reason: String): CommandResult {
        val sid = StoryboardId(storyboardId)
        val currentVersion = storyboardRepository.getCurrentVersion(sid)
            ?: return CommandResult.NotFound("Storyboard not found: $storyboardId")

        val event = ProjectionRebuildRequested(
            storyboardId = storyboardId,
            aggregateVersion = currentVersion + 1,
            reason = reason
        )

        eventLogRepository.append(event)
        eventLogRepository.appendOutbox(event, eventTopic)

        return CommandResult.Success(storyboardId, currentVersion + 1, event.eventId)
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
