package com.calmcut.studio.worker

import com.calmcut.studio.analysis.AnalysisResult
import com.calmcut.studio.projection.StoryboardState

/**
 * Persists and retrieves the projected [StoryboardState] and the latest
 * [AnalysisResult] per storyboard. Backed by the projection tables in
 * production and by in-memory maps in tests. Supports full reset for
 * "从零重建投影".
 */
interface ProjectionStore {
    suspend fun loadState(storyboardId: String): StoryboardState?
    suspend fun saveState(state: StoryboardState)
    suspend fun loadAnalysis(storyboardId: String): AnalysisResult?
    suspend fun saveAnalysis(result: AnalysisResult)
    suspend fun resetAll()
}

/** Publishes analysis results downstream (Kafka results topic in production). */
interface ResultPublisher {
    suspend fun publish(result: AnalysisResult)
}
