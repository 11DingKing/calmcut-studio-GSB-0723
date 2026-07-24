package com.calmcut.studio.testutil

import com.calmcut.studio.config.AppConfig
import com.calmcut.studio.domain.model.StoryboardSegment
import java.util.UUID
import kotlin.random.Random

object TestFixtures {
    fun defaultAnalysisConfig(): AppConfig.AnalysisConfig = AppConfig.AnalysisConfig(
        ruleVersion = "1.0.0",
        windowSizeSeconds = 60,
        maxReversalsPerWindow = 6,
        consecutiveIntensityCount = 3,
        maxIntensity = 5,
        minAvgShotSeconds = 3.0,
        bufferIntensityThreshold = 3,
        incrementalMode = true,
        driftCheckEnabled = false,
        driftCheckSampleRate = 1.0
    )

    fun segment(
        id: String = UUID.randomUUID().toString().take(8),
        timelineId: String = "test-timeline",
        orderIndex: Int = 0,
        startTimeMs: Long = orderIndex * 5000L,
        durationMs: Long = 5000L,
        intensity: Int = 3,
        isReversal: Boolean = false,
        isKnowledgePoint: Boolean = false,
        isDeclineInducement: Boolean = false,
        knowledgePointId: String? = null,
        isOriginal: Boolean = true,
        content: String? = null,
        version: Long = 1
    ) = StoryboardSegment(
        id = id,
        timelineId = timelineId,
        version = version,
        orderIndex = orderIndex,
        startTimeMs = startTimeMs,
        endTimeMs = startTimeMs + durationMs,
        intensity = intensity,
        isReversal = isReversal,
        isKnowledgePoint = isKnowledgePoint,
        isDeclineInducement = isDeclineInducement,
        knowledgePointId = knowledgePointId,
        isOriginal = isOriginal,
        content = content
    )

    fun continuousSegments(
        count: Int,
        timelineId: String = "test-timeline",
        durationMs: Long = 5000L,
        startIntensity: Int = 3,
        startIndex: Int = 0
    ): List<StoryboardSegment> {
        return (0 until count).map { i ->
            segment(
                id = "seg-${startIndex + i}",
                timelineId = timelineId,
                orderIndex = startIndex + i,
                startTimeMs = (startIndex + i) * durationMs,
                durationMs = durationMs,
                intensity = startIntensity
            )
        }
    }

    fun reversalsWithinWindow(
        count: Int,
        windowSeconds: Int = 60,
        timelineId: String = "test-timeline"
    ): List<StoryboardSegment> {
        val spacing = (windowSeconds * 1000L) / (count + 1)
        return (0 until count).map { i ->
            segment(
                id = "rev-$i",
                timelineId = timelineId,
                orderIndex = i,
                startTimeMs = i * spacing,
                durationMs = 2000L,
                intensity = 4,
                isReversal = true
            )
        }
    }

    fun randomSegments(
        count: Int,
        timelineId: String = "test-timeline",
        seed: Long = 42,
        durationMs: Long = 4000L
    ): List<StoryboardSegment> {
        val rng = Random(seed)
        return (0 until count).map { i ->
            segment(
                id = "rand-$i",
                timelineId = timelineId,
                orderIndex = i,
                startTimeMs = i * durationMs,
                durationMs = durationMs,
                intensity = rng.nextInt(1, 6),
                isReversal = rng.nextDouble() < 0.15,
                isKnowledgePoint = rng.nextDouble() < 0.2,
                isDeclineInducement = rng.nextDouble() < 0.05
            )
        }
    }

    fun modifySegment(
        original: StoryboardSegment,
        intensity: Int? = null,
        isReversal: Boolean? = null,
        isKnowledgePoint: Boolean? = null,
        isDeclineInducement: Boolean? = null,
        startTimeMs: Long? = null,
        endTimeMs: Long? = null,
        orderIndex: Int? = null
    ) = original.copy(
        intensity = intensity ?: original.intensity,
        isReversal = isReversal ?: original.isReversal,
        isKnowledgePoint = isKnowledgePoint ?: original.isKnowledgePoint,
        isDeclineInducement = isDeclineInducement ?: original.isDeclineInducement,
        startTimeMs = startTimeMs ?: original.startTimeMs,
        endTimeMs = endTimeMs ?: original.endTimeMs,
        orderIndex = orderIndex ?: original.orderIndex
    )
}
