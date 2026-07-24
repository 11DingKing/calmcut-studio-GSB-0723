package com.calmcut.service

import com.calmcut.domain.*
import com.calmcut.domain.events.*
import com.calmcut.domain.rules.RiskRuleConfig
import com.calmcut.infrastructure.messaging.MessageHandlerResult
import com.calmcut.infrastructure.repository.AtomicWriteRepository
import com.calmcut.infrastructure.repository.RiskProjectionRepository
import com.calmcut.infrastructure.repository.StoryboardRepository
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

class EventProcessor(
    private val storyboardRepository: StoryboardRepository,
    private val atomicWriteRepository: AtomicWriteRepository,
    private val projectionRepository: RiskProjectionRepository,
    private val analysisEngine: RiskAnalysisEngine,
    private val knowledgePointValidator: KnowledgePointValidator,
    private val ruleConfig: RiskRuleConfig
) {
    suspend fun processEvent(event: DomainEvent, consumerGroup: String = "storyboard-risk-worker"): MessageHandlerResult {
        return try {
            val storyboardId = StoryboardId(event.storyboardId)

            val alreadyProcessed = atomicWriteRepository.getProcessedVersion(consumerGroup, event.storyboardId)
            if (event.aggregateVersion <= alreadyProcessed) {
                logger.debug { "Event ${event.eventId} v${event.aggregateVersion} already processed (last=$alreadyProcessed), skipping" }
                return MessageHandlerResult(success = true, processedVersion = event.aggregateVersion)
            }

            when (event) {
                is StoryboardCreated -> handleStoryboardCreated(event)
                is SegmentsBatchImported -> handleBatchImported(event, storyboardId)
                is SegmentAdded -> handleSegmentAdded(event, storyboardId)
                is SegmentUpdated -> handleSegmentUpdated(event, storyboardId)
                is SegmentDeleted -> handleSegmentDeleted(event, storyboardId)
                is SegmentsReordered -> handleReordered(event, storyboardId)
                is ImportJobCreated -> {
                    logger.debug { "Import job created: ${event.importJobId}" }
                }
                is ImportJobCompleted -> {
                    logger.debug { "Import job completed: ${event.importJobId}, triggering full analysis" }
                    analysisEngine.analyzeFull(storyboardId, event.aggregateVersion)
                }
                is ImportJobCancelled -> {
                    logger.info { "Import job cancelled: ${event.importJobId}" }
                }
                is ProjectionRebuildRequested -> {
                    logger.info { "Rebuilding projection for ${event.storyboardId}, reason: ${event.reason}" }
                    analysisEngine.analyzeFull(storyboardId, event.aggregateVersion)
                }
            }

            atomicWriteRepository.saveProcessedVersion(consumerGroup, event.storyboardId, event.aggregateVersion)

            MessageHandlerResult(
                success = true,
                processedVersion = event.aggregateVersion
            )
        } catch (e: Exception) {
            logger.error(e) { "Failed to process event ${event.eventId} (${event.eventType})" }
            MessageHandlerResult(
                success = false,
                error = e.message ?: "Unknown error"
            )
        }
    }

    private suspend fun handleStoryboardCreated(event: StoryboardCreated) {
        val storyboard = Storyboard(
            id = StoryboardId(event.storyboardId),
            externalId = event.externalId,
            title = event.title,
            currentVersion = event.aggregateVersion,
            originalKnowledgePoints = event.originalKnowledgePoints,
            segments = emptyList()
        )
        storyboardRepository.create(storyboard)
    }

    private suspend fun handleBatchImported(event: SegmentsBatchImported, storyboardId: StoryboardId) {
        val segments = event.segments.map { data ->
            StoryboardSegment(
                id = data.id?.let { SegmentId(it) } ?: SegmentId.generate(),
                storyboardId = storyboardId,
                order = data.order,
                startTimeMs = data.startTimeMs,
                endTimeMs = data.endTimeMs,
                stimulusIntensity = data.stimulusIntensity,
                hasReversal = data.hasReversal,
                isKnowledgePoint = data.isKnowledgePoint,
                knowledgePointId = data.knowledgePointId,
                hasScrollInducement = data.hasScrollInducement,
                contentType = ContentType.valueOf(data.contentType)
            )
        }

        storyboardRepository.insertSegments(storyboardId, segments)

        val kpCount = segments.count { it.isKnowledgePoint }
        storyboardRepository.updateKnowledgePointCount(storyboardId, kpCount)
        knowledgePointValidator.validateKnowledgePointIntegrity(storyboardId)
        analysisEngine.analyzeIncremental(storyboardId, event.aggregateVersion, event)
    }

    private suspend fun handleSegmentAdded(event: SegmentAdded, storyboardId: StoryboardId) {
        val data = event.segment
        val segment = StoryboardSegment(
            id = data.id?.let { SegmentId(it) } ?: SegmentId.generate(),
            storyboardId = storyboardId,
            order = data.order,
            startTimeMs = data.startTimeMs,
            endTimeMs = data.endTimeMs,
            stimulusIntensity = data.stimulusIntensity,
            hasReversal = data.hasReversal,
            isKnowledgePoint = data.isKnowledgePoint,
            knowledgePointId = data.knowledgePointId,
            hasScrollInducement = data.hasScrollInducement,
            contentType = ContentType.valueOf(data.contentType)
        )

        storyboardRepository.insertSegments(storyboardId, listOf(segment))

        if (segment.isKnowledgePoint) {
            val segments = storyboardRepository.getSegments(storyboardId)
            storyboardRepository.updateKnowledgePointCount(storyboardId, segments.count { it.isKnowledgePoint })
            knowledgePointValidator.validateKnowledgePointIntegrity(storyboardId)
        }
        analysisEngine.analyzeIncremental(storyboardId, event.aggregateVersion, event)
    }

    private suspend fun handleSegmentUpdated(event: SegmentUpdated, storyboardId: StoryboardId) {
        val segmentId = SegmentId(event.segmentId)
        storyboardRepository.updateSegment(storyboardId, segmentId, event.changes)

        if (event.changes.isKnowledgePoint != null || event.changes.knowledgePointId != null) {
            val segments = storyboardRepository.getSegments(storyboardId)
            storyboardRepository.updateKnowledgePointCount(storyboardId, segments.count { it.isKnowledgePoint })
            knowledgePointValidator.validateKnowledgePointIntegrity(storyboardId)
        }
        analysisEngine.analyzeIncremental(storyboardId, event.aggregateVersion, event)
    }

    private suspend fun handleSegmentDeleted(event: SegmentDeleted, storyboardId: StoryboardId) {
        val segmentId = SegmentId(event.segmentId)
        storyboardRepository.deleteSegment(storyboardId, segmentId)

        val segments = storyboardRepository.getSegments(storyboardId)
        storyboardRepository.updateKnowledgePointCount(storyboardId, segments.count { it.isKnowledgePoint })
        knowledgePointValidator.validateKnowledgePointIntegrity(storyboardId)
        analysisEngine.analyzeFull(storyboardId, event.aggregateVersion)
    }

    private suspend fun handleReordered(event: SegmentsReordered, storyboardId: StoryboardId) {
        storyboardRepository.reorderSegments(storyboardId, event.newOrder)
        analysisEngine.analyzeFull(storyboardId, event.aggregateVersion)
    }

    suspend fun rebuildProjectionFromScratch(storyboardId: StoryboardId): RiskAnalysisResult {
        projectionRepository.deleteProjection(storyboardId)
        val version = atomicWriteRepository.getProcessedVersion("storyboard-risk-worker", storyboardId.value)
        return analysisEngine.analyzeFull(storyboardId, version)
    }
}
