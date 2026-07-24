package com.calmcut.studio.worker

import com.calmcut.studio.analysis.AnalysisResult

/** Publishes analysis results downstream (Kafka results topic in production). */
interface ResultPublisher {
    suspend fun publish(result: AnalysisResult)
}
