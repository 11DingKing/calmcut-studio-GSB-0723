package com.calmcut.service

import com.calmcut.domain.*
import com.calmcut.infrastructure.repository.RiskProjectionRepository
import com.calmcut.infrastructure.repository.StoryboardRepository
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

class StoryboardQueryService(
    private val storyboardRepository: StoryboardRepository,
    private val projectionRepository: RiskProjectionRepository,
    private val analysisEngine: RiskAnalysisEngine,
    private val knowledgePointValidator: KnowledgePointValidator
) {
    suspend fun getStoryboard(id: StoryboardId): Storyboard? {
        val storyboard = storyboardRepository.findById(id) ?: return null
        val segments = storyboardRepository.getSegments(id)
        return storyboard.copy(segments = segments)
    }

    suspend fun getSegments(id: StoryboardId): List<StoryboardSegment> {
        return storyboardRepository.getSegments(id)
    }

    suspend fun getRiskAnalysis(id: StoryboardId): RiskAnalysisResult? {
        return projectionRepository.getProjection(id)
    }

    suspend fun getCurrentVersion(id: StoryboardId): Long? {
        return storyboardRepository.getCurrentVersion(id)
    }

    suspend fun verifyEquivalence(id: StoryboardId, version: Long): DriftCheckResult {
        return analysisEngine.verifyEquivalence(id, version)
    }

    suspend fun validateKnowledgePoints(id: StoryboardId): com.calmcut.service.KnowledgePointValidationResult {
        return knowledgePointValidator.validateKnowledgePointIntegrity(id)
    }
}
