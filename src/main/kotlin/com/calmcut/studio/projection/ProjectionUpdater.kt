package com.calmcut.studio.projection

import com.calmcut.studio.analysis.DriftResult
import com.calmcut.studio.analysis.RiskAnalyzer
import com.calmcut.studio.config.AppConfig
import com.calmcut.studio.db.DatabaseFactory
import com.calmcut.studio.domain.model.*
import com.calmcut.studio.event.*
import mu.KotlinLogging
import java.util.UUID

private val logger = KotlinLogging.logger {}

class ProjectionUpdater(
    private val repository: ProjectionRepository,
    private val analyzer: RiskAnalyzer,
    private val config: AppConfig.AnalysisConfig
) {
    suspend fun applyEvent(event: StoryboardEvent): ProjectionResult {
        return when (event) {
            is TimelineCreated -> applyTimelineCreated(event)
            is SegmentCreated -> applySegmentCreated(event)
            is SegmentUpdated -> applySegmentUpdated(event)
            is SegmentDeleted -> applySegmentDeleted(event)
            is BatchImported -> applyBatchImported(event)
            is TimelineReset -> applyTimelineReset(event)
        }
    }

    private suspend fun applyTimelineCreated(event: TimelineCreated): ProjectionResult {
        val state = TimelineState(
            timelineId = event.timelineId,
            version = event.version,
            segments = event.initialSegments,
            lastEventId = event.eventId
        )
        repository.upsertTimeline(state)
        if (event.initialSegments.isNotEmpty()) {
            repository.replaceSegments(event.timelineId, event.initialSegments)
        }

        val result = analyzer.analyzeFull(state)
        repository.saveFindings(result)
        repository.saveCheckpoint(event.timelineId, event.eventId, event.version)

        return ProjectionResult(event.timelineId, event.version, result, changedSegmentIds = event.initialSegments.map { it.id }.toSet())
    }

    private suspend fun applySegmentCreated(event: SegmentCreated): ProjectionResult {
        val oldState = repository.loadTimeline(event.timelineId)
            ?: TimelineState(event.timelineId, 0, emptyList(), null)

        val newSegments = oldState.segments + event.segment
        val newState = oldState.copy(
            version = event.version,
            segments = newSegments.sortedBy { it.orderIndex },
            lastEventId = event.eventId
        )

        repository.upsertTimeline(newState)
        repository.upsertSegment(event.segment)

        val previousFindings = repository.loadFindings(event.timelineId)
        val result = if (config.incrementalMode && oldState.segments.isNotEmpty()) {
            analyzer.analyzeIncremental(oldState, newState, previousFindings, setOf(event.segment.id))
        } else {
            analyzer.analyzeFull(newState)
        }
        repository.saveFindings(result)
        repository.saveCheckpoint(event.timelineId, event.eventId, event.version)

        return ProjectionResult(event.timelineId, event.version, result, changedSegmentIds = setOf(event.segment.id))
    }

    private suspend fun applySegmentUpdated(event: SegmentUpdated): ProjectionResult {
        val oldState = repository.loadTimeline(event.timelineId)
            ?: throw IllegalStateException("Timeline ${event.timelineId} not found for update")

        if (oldState.version != event.expectedVersion) {
            throw OptimisticLockException(
                "Version conflict: expected ${event.expectedVersion}, current ${oldState.version}"
            )
        }

        val oldSegment = event.previousVersion
        val newSegment = event.segment.copyWithVersion(event.version)
        val newSegments = oldState.segments.map {
            if (it.id == event.aggregateId) newSegment else it
        }.sortedBy { it.orderIndex }

        val newState = oldState.copy(
            version = event.version,
            segments = newSegments,
            lastEventId = event.eventId
        )

        repository.upsertTimeline(newState)
        repository.upsertSegment(newSegment)

        val previousFindings = repository.loadFindings(event.timelineId)
        val changedSegmentIds = setOf(event.aggregateId)
        val result = if (config.incrementalMode) {
            analyzer.analyzeIncremental(
                oldState.copy(segments = oldState.segments.map {
                    if (it.id == event.aggregateId) oldSegment else it
                }),
                newState,
                previousFindings,
                changedSegmentIds
            )
        } else {
            analyzer.analyzeFull(newState)
        }

        var drift: DriftResult? = null
        if (config.driftCheckEnabled && Math.random() < config.driftCheckSampleRate) {
            val fullResult = analyzer.analyzeFull(newState)
            drift = analyzer.checkDrift(result, fullResult)
            if (drift.hasDrift) {
                logger.warn { "Drift detected for ${event.timelineId} after event ${event.eventId}: ${drift.mismatches}" }
                repository.saveFindings(fullResult)
                return ProjectionResult(event.timelineId, event.version, fullResult, changedSegmentIds, drift)
            }
        }

        repository.saveFindings(result)
        repository.saveCheckpoint(event.timelineId, event.eventId, event.version)

        return ProjectionResult(event.timelineId, event.version, result, changedSegmentIds, drift)
    }

    private suspend fun applySegmentDeleted(event: SegmentDeleted): ProjectionResult {
        val oldState = repository.loadTimeline(event.timelineId)
            ?: throw IllegalStateException("Timeline ${event.timelineId} not found for delete")

        if (oldState.version != event.expectedVersion) {
            throw OptimisticLockException(
                "Version conflict: expected ${event.expectedVersion}, current ${oldState.version}"
            )
        }

        val newSegments = oldState.segments.filter { it.id != event.segmentId }
        val newState = oldState.copy(
            version = event.version,
            segments = newSegments,
            lastEventId = event.eventId
        )

        repository.upsertTimeline(newState)
        repository.deleteSegment(event.timelineId, event.segmentId)

        val previousFindings = repository.loadFindings(event.timelineId)
        val result = if (config.incrementalMode && newSegments.isNotEmpty()) {
            analyzer.analyzeIncremental(oldState, newState, previousFindings, setOf(event.segmentId))
        } else {
            analyzer.analyzeFull(newState)
        }
        repository.saveFindings(result)
        repository.saveCheckpoint(event.timelineId, event.eventId, event.version)

        return ProjectionResult(event.timelineId, event.version, result, changedSegmentIds = setOf(event.segmentId))
    }

    private suspend fun applyBatchImported(event: BatchImported): ProjectionResult {
        val oldState = repository.loadTimeline(event.timelineId)
        val newSegments = when (event.mode) {
            BatchImported.ImportMode.REPLACE -> event.segments
            BatchImported.ImportMode.APPEND -> (oldState?.segments ?: emptyList()) + event.segments
        }.sortedBy { it.orderIndex }

        val newState = TimelineState(
            timelineId = event.timelineId,
            version = event.version,
            segments = newSegments,
            lastEventId = event.eventId
        )

        repository.upsertTimeline(newState)
        when (event.mode) {
            BatchImported.ImportMode.REPLACE -> repository.replaceSegments(event.timelineId, newSegments)
            BatchImported.ImportMode.APPEND -> event.segments.forEach { repository.upsertSegment(it) }
        }

        val result = analyzer.analyzeFull(newState)
        repository.saveFindings(result)
        repository.saveCheckpoint(event.timelineId, event.eventId, event.version)

        return ProjectionResult(
            event.timelineId,
            event.version,
            result,
            changedSegmentIds = event.segments.map { it.id }.toSet()
        )
    }

    private suspend fun applyTimelineReset(event: TimelineReset): ProjectionResult {
        repository.resetTimeline(event.timelineId)
        val state = TimelineState(event.timelineId, event.version, emptyList(), event.eventId)
        val result = analyzer.analyzeFull(state)
        repository.saveCheckpoint(event.timelineId, event.eventId, event.version)

        return ProjectionResult(event.timelineId, event.version, result, changedSegmentIds = emptySet<String>())
    }
}

class OptimisticLockException(message: String) : RuntimeException(message)

data class ProjectionResult(
    val timelineId: String,
    val version: Long,
    val analysisResult: RiskAnalysisResult,
    val changedSegmentIds: Set<String>,
    val driftResult: DriftResult? = null
)
