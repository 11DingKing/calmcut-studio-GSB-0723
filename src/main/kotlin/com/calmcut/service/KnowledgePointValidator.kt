package com.calmcut.service

import com.calmcut.domain.StoryboardId
import com.calmcut.infrastructure.repository.StoryboardRepository
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

data class KnowledgePointValidationResult(
    val isValid: Boolean,
    val originalCount: Int?,
    val revisedCount: Int,
    val missingIds: List<String>,
    val extraIds: List<String>,
    val errors: List<String>
)

class KnowledgePointValidator(
    private val storyboardRepository: StoryboardRepository
) {
    suspend fun validateKnowledgePointIntegrity(storyboardId: StoryboardId): KnowledgePointValidationResult {
        val errors = mutableListOf<String>()

        val originalPoints = storyboardRepository.getOriginalKnowledgePoints(storyboardId)
        val segments = storyboardRepository.getSegments(storyboardId)

        val revisedKpIds = segments
            .filter { it.isKnowledgePoint }
            .mapNotNull { it.knowledgePointId }
            .distinct()
            .sorted()

        if (originalPoints == null) {
            return KnowledgePointValidationResult(
                isValid = true,
                originalCount = null,
                revisedCount = revisedKpIds.size,
                missingIds = emptyList(),
                extraIds = emptyList(),
                errors = emptyList()
            )
        }

        val originalSet = originalPoints.toSet()
        val revisedSet = revisedKpIds.toSet()

        val missing = originalSet - revisedSet
        val extra = revisedSet - originalSet

        if (missing.isNotEmpty()) {
            errors.add("Missing knowledge points from original: ${missing.joinToString(", ")}")
            logger.warn { "Storyboard ${storyboardId.value}: missing knowledge points: $missing" }
        }

        if (extra.isNotEmpty()) {
            errors.add("Unexpected knowledge points not in original: ${extra.joinToString(", ")}")
            logger.warn { "Storyboard ${storyboardId.value}: extra knowledge points: $extra" }
        }

        val duplicateIds = revisedKpIds.groupBy { it }
            .filter { it.value.size > 1 }
            .keys
        if (duplicateIds.isNotEmpty()) {
            errors.add("Duplicate knowledge point IDs found: ${duplicateIds.joinToString(", ")}")
        }

        val invalidMarkedSegments = segments.filter { it.isKnowledgePoint && it.knowledgePointId == null }
        if (invalidMarkedSegments.isNotEmpty()) {
            errors.add("${invalidMarkedSegments.size} segments marked as knowledge points but have no knowledgePointId")
        }

        val unmarkedSegments = segments.filter { !it.isKnowledgePoint && it.knowledgePointId != null }
        if (unmarkedSegments.isNotEmpty()) {
            errors.add("${unmarkedSegments.size} segments have knowledgePointId but are not marked as knowledge points")
        }

        val isValid = errors.isEmpty()

        return KnowledgePointValidationResult(
            isValid = isValid,
            originalCount = originalPoints.size,
            revisedCount = revisedKpIds.size,
            missingIds = missing.toList().sorted(),
            extraIds = extra.toList().sorted(),
            errors = errors
        )
    }
}
