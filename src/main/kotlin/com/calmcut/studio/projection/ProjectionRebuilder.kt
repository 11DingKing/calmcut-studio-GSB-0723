package com.calmcut.studio.projection

import com.calmcut.studio.analysis.DriftResult
import com.calmcut.studio.analysis.RiskAnalyzer
import com.calmcut.studio.event.EventStore
import com.calmcut.studio.event.eventJson
import com.calmcut.studio.event.StoryboardEvent
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

class ProjectionRebuilder(
    private val eventStore: EventStore,
    private val repository: ProjectionRepository,
    private val updater: ProjectionUpdater,
    private val analyzer: RiskAnalyzer
) {
    suspend fun rebuildFromScratch(timelineId: String): RebuildResult {
        logger.info { "Rebuilding projection from scratch for timeline $timelineId" }

        repository.resetTimeline(timelineId)

        val events = eventStore.getEventsForTimeline(timelineId, fromVersion = 0)
        var eventCount = 0
        var lastVersion = 0L
        var lastResult: com.calmcut.studio.domain.model.RiskAnalysisResult? = null

        for ((payload, version) in events) {
            val event = eventJson.decodeFromString<StoryboardEvent>(payload)
            val result = updater.applyEvent(event)
            lastResult = result.analysisResult
            lastVersion = version
            eventCount++

            if (eventCount % 100 == 0) {
                logger.debug { "Rebuilt $eventCount events for $timelineId" }
            }
        }

        logger.info { "Rebuilt projection for $timelineId: $eventCount events, version $lastVersion" }

        val state = repository.loadTimeline(timelineId)
        val drift = if (state != null && lastResult != null) {
            val fullResult = analyzer.analyzeFull(state)
            analyzer.checkDrift(lastResult, fullResult)
        } else null

        return RebuildResult(timelineId, eventCount, lastVersion, drift)
    }

    suspend fun rebuildAll(): List<RebuildResult> {
        val timelines = eventStore.getDistinctTimelineIds()
        logger.info { "Rebuilding all ${timelines.size} timelines from scratch" }
        return timelines.map { rebuildFromScratch(it) }
    }

    data class RebuildResult(
        val timelineId: String,
        val eventsProcessed: Int,
        val finalVersion: Long,
        val drift: DriftResult?
    )
}
