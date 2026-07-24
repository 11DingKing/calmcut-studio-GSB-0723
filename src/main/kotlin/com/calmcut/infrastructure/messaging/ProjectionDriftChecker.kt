package com.calmcut.infrastructure.messaging

import com.calmcut.domain.StoryboardId
import com.calmcut.infrastructure.db.Storyboards
import com.calmcut.infrastructure.repository.AtomicWriteRepository
import com.calmcut.infrastructure.repository.RiskProjectionRepository
import com.calmcut.infrastructure.repository.StoryboardRepository
import com.calmcut.service.EventProcessor
import com.calmcut.service.RiskAnalysisEngine
import kotlinx.coroutines.*
import mu.KotlinLogging
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.experimental.newSuspendedTransaction

private val logger = KotlinLogging.logger {}

class ProjectionDriftChecker(
    private val projectionRepository: RiskProjectionRepository,
    private val storyboardRepository: StoryboardRepository,
    private val analysisEngine: RiskAnalysisEngine,
    private val eventProcessor: EventProcessor,
    private val atomicWriteRepository: AtomicWriteRepository
) {
    fun start(scope: CoroutineScope, intervalSeconds: Long = 3600) {
        scope.launch {
            logger.info { "Projection drift checker started (interval=${intervalSeconds}s)" }
            while (isActive) {
                try {
                    delay(intervalSeconds * 1000)
                    checkAllProjections()
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    logger.error(e) { "Error in drift checker" }
                    delay(60000)
                }
            }
        }
    }

    private suspend fun checkAllProjections() {
        logger.info { "Starting periodic projection drift check..." }

        val storyboardIds = newSuspendedTransaction {
            Storyboards.selectAll()
                .map { it[Storyboards.id].toString() }
        }

        var drifted = 0
        for (idStr in storyboardIds) {
            try {
                val id = StoryboardId(idStr)
                val version = atomicWriteRepository.getProcessedVersion("storyboard-risk-worker", idStr)
                val result = analysisEngine.verifyEquivalence(id, version)
                if (result.hasDrift) {
                    drifted++
                    logger.warn { "Drift detected for storyboard $idStr, auto-repaired via full recomputation" }
                }
                delay(100)
            } catch (e: Exception) {
                logger.error(e) { "Failed to check drift for storyboard $idStr" }
            }
        }

        logger.info { "Drift check completed: ${storyboardIds.size} checked, $drifted drifted" }
    }

    suspend fun forceRebuildAll(): Int {
        val storyboardIds = newSuspendedTransaction {
            Storyboards.selectAll()
                .map { it[Storyboards.id].toString() }
        }

        var rebuilt = 0
        for (idStr in storyboardIds) {
            try {
                val id = StoryboardId(idStr)
                eventProcessor.rebuildProjectionFromScratch(id)
                rebuilt++
            } catch (e: Exception) {
                logger.error(e) { "Failed to rebuild projection for $idStr" }
            }
        }
        return rebuilt
    }
}
